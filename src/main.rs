// GoTTY WebSocket client (Rust, Unix only)
//
// GoTTY protocol (text frames; v2.0 numbering)
//   Client -> Server: '1' Input, '2' Ping, '3' ResizeTerminal
//   Server -> Client: '1' Output (base64), '2' Pong, '3' SetWindowTitle,
//                     '4' SetPreferences, '5' SetReconnect
//   The first client message is plain JSON: {"AuthToken": "..."} (no type prefix).
//
// Exit the local client with Ctrl+]  (Ctrl+C / Ctrl+D go to the remote shell).

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use clap::Parser;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, size};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

const EXIT_KEY: u8 = 0x1d; // Ctrl+]
const PING_EVERY: Duration = Duration::from_secs(10);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<Ws, Message>;
type WsStream = SplitStream<Ws>;

// --- CLI ---------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(author, version, about = "WebSocket terminal client for GoTTY")]
struct Args {
    /// WebSocket URL
    #[arg(long, default_value = "ws://127.0.0.1:8080/ws")]
    url: String,

    /// Basic Auth username
    #[arg(long)]
    username: Option<String>,

    /// Basic Auth password
    #[arg(long)]
    password: Option<String>,

    /// Max connection attempts (ignored with --reconnect)
    #[arg(long, default_value_t = 3)]
    retries: usize,

    /// Keep reconnecting forever
    #[arg(long)]
    reconnect: bool,

    /// Enable debug output
    #[arg(long)]
    debug: bool,
}

static DEBUG: AtomicBool = AtomicBool::new(false);
/// True while a session owns the terminal (raw mode). The stdin thread only reads then.
static ACTIVE: AtomicBool = AtomicBool::new(false);

fn debug_print(msg: impl AsRef<str>) {
    if DEBUG.load(Ordering::Relaxed) {
        // '\r\n' because the terminal may be in raw mode
        eprint!("[DEBUG] {}\r\n", msg.as_ref());
    }
}

// --- Errors ------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
enum GottyError {
    /// Authentication failed; retrying will not help.
    #[error("Auth failed: {0}")]
    Auth(String),
}

enum End {
    UserQuit,
    StdinEof,
    ServerClosed,
}

// --- Terminal / stdin --------------------------------------------------

/// Enables raw mode for the lifetime of the guard (restored on drop, also on panic).
struct RawGuard;

impl RawGuard {
    fn enable() -> Result<Self> {
        enable_raw_mode().context("failed to enable raw mode")?;
        ACTIVE.store(true, Ordering::Release);
        Ok(RawGuard)
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Release);
        let _ = disable_raw_mode();
    }
}

/// Reads fd 0 on a plain thread (so a blocked read can never stall the tokio runtime).
/// It polls with a timeout and only reads while a session is active, so no read is
/// left pending across raw-mode switches. An empty Vec signals EOF.
fn spawn_stdin_thread(tx: UnboundedSender<Vec<u8>>) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1024];
        loop {
            if !ACTIVE.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            let mut pfd = libc::pollfd { fd: 0, events: libc::POLLIN, revents: 0 };
            let r = unsafe { libc::poll(&mut pfd, 1, 100) };
            if r < 0 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            if r == 0 || !ACTIVE.load(Ordering::Acquire) {
                continue;
            }
            let n = unsafe { libc::read(0, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n < 0 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            if n == 0 {
                let _ = tx.send(Vec::new());
                break;
            }
            if tx.send(buf[..n as usize].to_vec()).is_err() {
                break;
            }
        }
    });
}

/// Incremental UTF-8 decoding: keeps an incomplete trailing sequence in `pending`
/// so multibyte characters split across reads are not corrupted.
fn decode_utf8(pending: &mut Vec<u8>, data: &[u8]) -> String {
    pending.extend_from_slice(data);
    let mut out = String::new();
    let mut start = 0;
    loop {
        match std::str::from_utf8(&pending[start..]) {
            Ok(s) => {
                out.push_str(s);
                start = pending.len();
                break;
            }
            Err(e) => {
                let valid = e.valid_up_to();
                out.push_str(&String::from_utf8_lossy(&pending[start..start + valid]));
                match e.error_len() {
                    Some(n) => {
                        out.push('\u{FFFD}');
                        start += valid + n;
                    }
                    None => {
                        start += valid; // incomplete tail: wait for more bytes
                        break;
                    }
                }
            }
        }
    }
    pending.drain(..start);
    out
}

// --- Auth token --------------------------------------------------------

/// Fetches the token from <prefix>/auth_token.js. Returns None when no auth is needed.
async fn fetch_auth_token(
    ws_url: &str,
    username: &Option<String>,
    password: &Option<String>,
) -> Result<Option<String>> {
    let mut u = Url::parse(ws_url).context("invalid URL")?;
    let scheme = if u.scheme() == "wss" { "https" } else { "http" };
    u.set_scheme(scheme)
        .map_err(|_| anyhow!("cannot convert URL scheme to {scheme}"))?;
    // Replace the last path segment (e.g. "/prefix/ws" -> "/prefix/auth_token.js")
    let dir = {
        let p = u.path();
        match p.rfind('/') {
            Some(i) => p[..=i].to_string(),
            None => "/".to_string(),
        }
    };
    u.set_path(&format!("{dir}auth_token.js"));
    u.set_query(None);
    u.set_fragment(None);
    debug_print(format!("Fetching auth token from: {u}"));

    let creds = match (username, password) {
        (Some(user), Some(pass)) => Some((user, pass)),
        _ => None,
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut req = client.get(u.as_str());
    if let Some((user, pass)) = creds {
        req = req.basic_auth(user, Some(pass));
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) if creds.is_none() => {
            debug_print(format!("auth_token.js unavailable ({e}) - assuming no auth"));
            return Ok(None);
        }
        Err(e) => bail!("Failed to fetch auth token: {e}"),
    };

    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(GottyError::Auth("HTTP 401 - check credentials".into()).into());
    }
    if status == reqwest::StatusCode::NOT_FOUND && creds.is_none() {
        debug_print("auth_token.js not found - assuming no auth");
        return Ok(None);
    }
    if !status.is_success() {
        bail!("auth_token.js returned HTTP {status}");
    }

    let body = resp.text().await?;
    let token = parse_token(&body);
    debug_print(if token.is_some() { "Auth token fetched" } else { "No auth token in response" });
    Ok(token)
}

fn parse_token(body: &str) -> Option<String> {
    const MARK: &str = "var gotty_auth_token = '";
    let rest = &body[body.find(MARK)? + MARK.len()..];
    let token = &rest[..rest.find('\'')?];
    if token.is_empty() { None } else { Some(token.to_string()) }
}

// --- Session -----------------------------------------------------------

async fn send_resize(sink: &mut WsSink) -> Result<()> {
    if let Ok((cols, rows)) = size() {
        let body = serde_json::json!({ "columns": cols, "rows": rows }).to_string();
        sink.send(Message::Text(format!("3{body}").into())).await?;
        debug_print(format!("-> Resize: {cols}x{rows}"));
    }
    Ok(())
}

/// Handles one server message payload (type byte + body).
fn handle_payload(data: &[u8]) -> Result<()> {
    let Some((&kind, body)) = data.split_first() else {
        return Ok(());
    };
    match kind {
        b'1' => match STANDARD.decode(body) {
            Ok(out) => {
                let mut so = std::io::stdout().lock();
                so.write_all(&out)?;
                so.flush()?;
            }
            Err(e) => debug_print(format!("Failed to decode base64 output: {e}")),
        },
        b'2' => debug_print("<- Pong"),
        b'3' => {
            let title = String::from_utf8_lossy(body);
            let mut so = std::io::stdout().lock();
            write!(so, "\x1b]0;{title}\x07")?;
            so.flush()?;
        }
        b'4' => debug_print("<- SetPreferences"),
        b'5' => debug_print("<- SetReconnect"),
        other => debug_print(format!("<- Unknown message type: {other:#x}")),
    }
    Ok(())
}

async fn session_loop(
    sink: &mut WsSink,
    stream: &mut WsStream,
    stdin_rx: &mut UnboundedReceiver<Vec<u8>>,
    token: Option<&str>,
) -> Result<End> {
    // First message: plain JSON, no type prefix
    let init = match token {
        Some(t) => serde_json::json!({ "AuthToken": t }).to_string(),
        None => "{}".to_string(),
    };
    sink.send(Message::Text(init.into())).await?;
    send_resize(sink).await?;

    let mut winch = signal(SignalKind::window_change())?;
    let mut ping = interval_at(Instant::now() + PING_EVERY, PING_EVERY);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pending = Vec::new();

    loop {
        tokio::select! {
            msg = stream.next() => match msg {
                None => return Ok(End::ServerClosed),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(Message::Close(_))) => return Ok(End::ServerClosed),
                Some(Ok(Message::Text(t))) => handle_payload(t.as_bytes())?,
                Some(Ok(Message::Binary(b))) => handle_payload(&b[..])?,
                Some(Ok(_)) => {} // Ping/Pong frames are answered by tungstenite itself
            },
            data = stdin_rx.recv() => {
                let Some(data) = data else { return Ok(End::StdinEof) };
                if data.is_empty() {
                    return Ok(End::StdinEof);
                }
                if data.contains(&EXIT_KEY) {
                    return Ok(End::UserQuit);
                }
                let text = decode_utf8(&mut pending, &data);
                if !text.is_empty() {
                    sink.send(Message::Text(format!("1{text}").into())).await?;
                }
            },
            _ = ping.tick() => {
                sink.send(Message::Text("2".into())).await?;
            },
            _ = winch.recv() => {
                send_resize(sink).await?;
            },
        }
    }
}

async fn run_session(args: &Args, stdin_rx: &mut UnboundedReceiver<Vec<u8>>) -> Result<End> {
    // Fetched on every attempt: tokens may expire between reconnects
    let token = fetch_auth_token(&args.url, &args.username, &args.password).await?;

    let mut req = args
        .url
        .as_str()
        .into_client_request()
        .context("invalid WebSocket URL")?;
    if let (Some(u), Some(p)) = (&args.username, &args.password) {
        let v = format!("Basic {}", STANDARD.encode(format!("{u}:{p}")));
        req.headers_mut().insert("Authorization", HeaderValue::from_str(&v)?);
    }

    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .context("WebSocket handshake failed")?;
    eprintln!("✅ Connected to {}  (exit: Ctrl+])", args.url);
    let (mut sink, mut stream) = ws.split();

    let guard = RawGuard::enable()?;
    while stdin_rx.try_recv().is_ok() {} // drop input typed while disconnected

    let result = session_loop(&mut sink, &mut stream, stdin_rx, token.as_deref()).await;
    let _ = sink.close().await;
    drop(guard); // restore the terminal before anything is printed
    result
}

// --- Main --------------------------------------------------------------

async fn run(args: Args) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        bail!("stdin is not a TTY");
    }
    let (tx, mut rx) = unbounded_channel();
    spawn_stdin_thread(tx);

    let max_attempts = args.retries.max(1);
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        debug_print(format!("Connection attempt #{attempt}"));

        match run_session(&args, &mut rx).await {
            Ok(End::UserQuit) | Ok(End::StdinEof) => {
                eprintln!("\n🔌 Disconnected. Terminal restored.");
                return Ok(());
            }
            Ok(End::ServerClosed) => {
                eprintln!("\n🔌 Session ended by server.");
                if !args.reconnect {
                    return Ok(());
                }
                attempt = 0;
                eprintln!("   Reconnecting in 2s...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => {
                if matches!(e.downcast_ref::<GottyError>(), Some(GottyError::Auth(_))) {
                    return Err(e);
                }
                if !args.reconnect && attempt >= max_attempts {
                    return Err(e.context(format!("failed after {attempt} attempts")));
                }
                let total = if args.reconnect { "∞".to_string() } else { max_attempts.to_string() };
                eprintln!("\n⚠️  Attempt {attempt}/{total} failed: {e:#}");
                // 1s, 2s, 4s, ... capped at 30s
                let wait = (1u64 << (attempt - 1).min(5) as u32).min(30);
                eprintln!("   Retrying in {wait}s...");
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    DEBUG.store(args.debug, Ordering::Relaxed);
    // wss:// needs a process-wide rustls crypto provider (ignore "already installed")
    let _ = rustls::crypto::ring::default_provider().install_default();

    match run(args).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("❌ Error: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
