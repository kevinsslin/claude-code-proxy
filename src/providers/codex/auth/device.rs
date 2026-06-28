use std::time::{Duration, Instant};

use super::constants::{CLIENT_ID, DEVICE_POLL_SAFETY_MARGIN_MS, ISSUER};
use super::jwt::TokenResponse;

const MAX_DEVICE_POLL_WAIT: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------------------
// Sleeper abstraction (injectable for tests)
// ---------------------------------------------------------------------------

pub trait Sleeper: Send + Sync {
    fn sleep(&self, dur: Duration);
}

pub struct StdSleeper;

impl Sleeper for StdSleeper {
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

// ---------------------------------------------------------------------------
// Device init response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DeviceInit {
    pub device_auth_id: String,
    pub user_code: String,
    #[serde(default)]
    pub interval: String,
}

// ---------------------------------------------------------------------------
// Device token poll response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DeviceTokenPoll {
    pub authorization_code: String,
    pub code_verifier: String,
}

// ---------------------------------------------------------------------------
// Device auth client
// ---------------------------------------------------------------------------

pub struct DeviceAuthClient {
    issuer: String,
    client: reqwest::blocking::Client,
    sleeper: Box<dyn Sleeper>,
    max_wait: Duration,
}

impl DeviceAuthClient {
    pub fn new() -> Self {
        Self::with_issuer(ISSUER)
    }

    pub fn with_issuer(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            sleeper: Box::new(StdSleeper),
            max_wait: MAX_DEVICE_POLL_WAIT,
        }
    }

    #[cfg(test)]
    pub fn with_sleeper(mut self, sleeper: Box<dyn Sleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    pub fn init(&self) -> Result<DeviceInit, anyhow::Error> {
        let body = serde_json::json!({ "client_id": CLIENT_ID });
        let resp = self
            .client
            .post(format!("{}/api/accounts/deviceauth/usercode", self.issuer))
            .json(&body)
            .send()
            .map_err(|e| anyhow::anyhow!("Device init network error: {e}"))?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let text = resp.text().unwrap_or_default();
            anyhow::bail!("Device init failed: {status} {text}");
        }

        let init: DeviceInit = resp
            .json()
            .map_err(|e| anyhow::anyhow!("failed to parse device init response: {e}"))?;
        Ok(init)
    }

    fn poll_token(
        &self,
        device_auth_id: &str,
        user_code: &str,
    ) -> Result<Option<DeviceTokenPoll>, anyhow::Error> {
        let body = serde_json::json!({
            "device_auth_id": device_auth_id,
            "user_code": user_code,
        });
        let resp = self
            .client
            .post(format!("{}/api/accounts/deviceauth/token", self.issuer))
            .json(&body)
            .send()
            .map_err(|e| anyhow::anyhow!("Device poll network error: {e}"))?;

        let status = resp.status().as_u16();
        if resp.status().is_success() {
            let poll: DeviceTokenPoll = resp
                .json()
                .map_err(|e| anyhow::anyhow!("failed to parse device poll response: {e}"))?;
            return Ok(Some(poll));
        }
        if status == 403 || status == 404 {
            return Ok(None);
        }
        let text = resp.text().unwrap_or_default();
        anyhow::bail!("Device poll failed: {status} {text}");
    }

    fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenResponse, anyhow::Error> {
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code),
            (
                "redirect_uri",
                &format!("{}/deviceauth/callback", self.issuer),
            ),
            ("client_id", CLIENT_ID),
            ("code_verifier", code_verifier),
        ];
        let resp = self
            .client
            .post(format!("{}/oauth/token", self.issuer))
            .form(&form)
            .send()
            .map_err(|e| anyhow::anyhow!("Token exchange network error: {e}"))?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let text = resp.text().unwrap_or_default();
            anyhow::bail!("Token exchange failed: {status} {text}");
        }

        let tokens: TokenResponse = resp
            .json()
            .map_err(|e| anyhow::anyhow!("failed to parse token exchange response: {e}"))?;
        Ok(tokens)
    }

    fn parse_interval(interval: &str) -> Duration {
        let ms = match interval.parse::<i64>() {
            Ok(v) if v > 0 => v as u64 * 1000,
            Ok(0) => 5000,
            Ok(v) if v < 0 => 1000,
            Ok(_) => 5000,
            Err(_) => 5000,
        };
        Duration::from_millis(ms)
    }

    pub fn run(&self) -> Result<TokenResponse, anyhow::Error> {
        let init = self.init()?;

        println!(
            "\nVisit: {}/codex/device\nEnter code: {}\n",
            self.issuer, init.user_code
        );

        let sleep_dur = Self::parse_interval(&init.interval)
            + Duration::from_millis(DEVICE_POLL_SAFETY_MARGIN_MS);
        let deadline = Instant::now() + self.max_wait;

        loop {
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "Device auth timed out after {} seconds",
                    self.max_wait.as_secs()
                );
            }

            match self.poll_token(&init.device_auth_id, &init.user_code)? {
                Some(poll) => {
                    let tokens =
                        self.exchange_code(&poll.authorization_code, &poll.code_verifier)?;
                    return Ok(tokens);
                }
                None => {
                    self.sleeper.sleep(sleep_dur);
                }
            }
        }
    }
}

impl Default for DeviceAuthClient {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// A sleeper that records durations instead of sleeping.
    #[derive(Default)]
    pub(crate) struct MockSleeper {
        pub slept: Mutex<Vec<Duration>>,
    }

    impl Sleeper for MockSleeper {
        fn sleep(&self, dur: Duration) {
            self.slept.lock().unwrap().push(dur);
        }
    }

    /// A minimal HTTP test server for the device auth flow.
    struct MockDeviceServer {
        url: String,
        #[allow(dead_code)]
        shutdown: Arc<AtomicBool>,
        saw_init: Arc<AtomicBool>,
        saw_token_exchange: Arc<AtomicBool>,
        saw_poll: Arc<AtomicBool>,
    }

    enum DeviceScenario {
        SuccessAfterPending,
        InitFailure,
        PollFailure,
        TokenExchangeFailure,
        InitThenTimeout,
    }

    fn json_response(status: u16, body: &str) -> String {
        let reason = match status {
            200 => "OK",
            403 => "Forbidden",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "Unknown",
        };
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        )
    }

    impl MockDeviceServer {
        fn new(scenario: DeviceScenario) -> Self {
            let saw_init = Arc::new(AtomicBool::new(false));
            let saw_token_exchange = Arc::new(AtomicBool::new(false));
            let saw_poll = Arc::new(AtomicBool::new(false));
            let shutdown = Arc::new(AtomicBool::new(false));

            let si = saw_init.clone();
            let ste = saw_token_exchange.clone();
            let sp = saw_poll.clone();
            let sd = shutdown.clone();
            let url: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));

            let url_clone = url.clone();
            std::thread::spawn(move || {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
                let addr = listener.local_addr().unwrap();
                *url_clone.lock().unwrap() = Some(format!("http://{addr}"));

                listener.set_nonblocking(true).expect("set nonblocking");

                loop {
                    if sd.load(Ordering::Relaxed) {
                        return;
                    }
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            use std::io::{Read, Write};
                            let mut buf = [0; 4096];
                            let n = match stream.read(&mut buf) {
                                Ok(n) if n > 0 => n,
                                _ => continue,
                            };
                            let request = String::from_utf8_lossy(&buf[..n]);

                            let response = match scenario {
                                DeviceScenario::SuccessAfterPending => {
                                    Self::handle_success_after_pending(&request, &si, &sp, &ste)
                                }
                                DeviceScenario::InitFailure => {
                                    Self::handle_init_failure(&request, &si)
                                }
                                DeviceScenario::PollFailure => {
                                    Self::handle_poll_failure(&request, &si, &sp)
                                }
                                DeviceScenario::TokenExchangeFailure => {
                                    Self::handle_token_exchange_failure(&request, &si, &sp, &ste)
                                }
                                DeviceScenario::InitThenTimeout => {
                                    Self::handle_init_then_timeout(&request, &si)
                                }
                            };

                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.flush();
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            });

            // Wait for server to be ready
            std::thread::sleep(Duration::from_millis(100));
            let url = url.lock().unwrap().clone().unwrap();

            Self {
                url,
                shutdown,
                saw_init,
                saw_token_exchange,
                saw_poll,
            }
        }

        fn handle_success_after_pending(
            request: &str,
            saw_init: &AtomicBool,
            saw_poll: &AtomicBool,
            saw_exchange: &AtomicBool,
        ) -> String {
            if request.contains("/api/accounts/deviceauth/usercode") {
                saw_init.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"2"}"#,
                )
            } else if request.contains("/api/accounts/deviceauth/token") {
                saw_poll.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"authorization_code":"auth_code_1","code_verifier":"verifier_1"}"#,
                )
            } else if request.contains("/oauth/token") {
                saw_exchange.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"access_token":"at_1","refresh_token":"rt_1","expires_in":3600,"id_token":"fake_id_token"}"#,
                )
            } else {
                Self::json_response(404, r#"{"error":"not found"}"#)
            }
        }

        fn handle_init_failure(request: &str, saw_init: &AtomicBool) -> String {
            if request.contains("/api/accounts/deviceauth/usercode") {
                saw_init.store(true, Ordering::Relaxed);
                Self::json_response(500, r#"{"error":"server error"}"#)
            } else {
                Self::json_response(404, r#"{"error":"not found"}"#)
            }
        }

        fn handle_poll_failure(
            request: &str,
            saw_init: &AtomicBool,
            saw_poll: &AtomicBool,
        ) -> String {
            if request.contains("/api/accounts/deviceauth/usercode") {
                saw_init.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"2"}"#,
                )
            } else if request.contains("/api/accounts/deviceauth/token") {
                saw_poll.store(true, Ordering::Relaxed);
                Self::json_response(500, r#"{"error":"poll error"}"#)
            } else {
                Self::json_response(404, r#"{"error":"not found"}"#)
            }
        }

        fn handle_token_exchange_failure(
            request: &str,
            saw_init: &AtomicBool,
            saw_poll: &AtomicBool,
            saw_exchange: &AtomicBool,
        ) -> String {
            if request.contains("/api/accounts/deviceauth/usercode") {
                saw_init.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"2"}"#,
                )
            } else if request.contains("/api/accounts/deviceauth/token") {
                saw_poll.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"authorization_code":"auth_code_1","code_verifier":"verifier_1"}"#,
                )
            } else if request.contains("/oauth/token") {
                saw_exchange.store(true, Ordering::Relaxed);
                Self::json_response(500, r#"{"error":"exchange failed"}"#)
            } else {
                Self::json_response(404, r#"{"error":"not found"}"#)
            }
        }

        fn handle_init_then_timeout(request: &str, saw_init: &AtomicBool) -> String {
            if request.contains("/api/accounts/deviceauth/usercode") {
                saw_init.store(true, Ordering::Relaxed);
                Self::json_response(
                    200,
                    r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"99999"}"#,
                )
            } else {
                // Return 403 to keep polling
                Self::json_response(403, r#"{"error":"pending"}"#)
            }
        }

        fn json_response(status: u16, body: &str) -> String {
            let reason = match status {
                200 => "OK",
                403 => "Forbidden",
                404 => "Not Found",
                500 => "Internal Server Error",
                _ => "Unknown",
            };
            format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            )
        }
    }

    #[test]
    fn device_flow_success_after_pending() {
        let server = MockDeviceServer::new(DeviceScenario::SuccessAfterPending);
        let client = DeviceAuthClient::with_issuer(&server.url);
        let tokens = client.run().unwrap();
        assert_eq!(tokens.access_token, "at_1");
        assert_eq!(tokens.refresh_token, "rt_1");
        assert!(server.saw_init.load(Ordering::Relaxed));
        assert!(server.saw_poll.load(Ordering::Relaxed));
        assert!(server.saw_token_exchange.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_reports_init_failure() {
        let server = MockDeviceServer::new(DeviceScenario::InitFailure);
        let err = DeviceAuthClient::with_issuer(&server.url)
            .run()
            .unwrap_err();
        assert!(
            err.to_string().contains("Device init failed"),
            "expected init failure, got: {err}"
        );
        assert!(server.saw_init.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_reports_poll_failure() {
        let server = MockDeviceServer::new(DeviceScenario::PollFailure);
        let err = DeviceAuthClient::with_issuer(&server.url)
            .run()
            .unwrap_err();
        assert!(
            err.to_string().contains("Device poll failed"),
            "expected poll failure, got: {err}"
        );
        assert!(server.saw_init.load(Ordering::Relaxed));
        assert!(server.saw_poll.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_reports_token_exchange_failure() {
        let server = MockDeviceServer::new(DeviceScenario::TokenExchangeFailure);
        let err = DeviceAuthClient::with_issuer(&server.url)
            .run()
            .unwrap_err();
        assert!(
            err.to_string().contains("Token exchange failed"),
            "expected token exchange failure, got: {err}"
        );
        assert!(server.saw_init.load(Ordering::Relaxed));
        assert!(server.saw_poll.load(Ordering::Relaxed));
        assert!(server.saw_token_exchange.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_times_out() {
        let server = MockDeviceServer::new(DeviceScenario::InitThenTimeout);
        let client = DeviceAuthClient {
            issuer: server.url.clone(),
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            sleeper: Box::new(MockSleeper::default()),
            max_wait: Duration::from_millis(100),
        };
        let err = client.run().unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected timeout, got: {err}"
        );
    }

    #[test]
    fn device_flow_polls_403_and_continues() {
        let saw_poll_403 = Arc::new(AtomicBool::new(false));
        let saw_init = Arc::new(AtomicBool::new(false));
        let saw_exchange = Arc::new(AtomicBool::new(false));
        let saw_second_poll = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let si = saw_init.clone();
        let sp403 = saw_poll_403.clone();
        let sp2 = saw_second_poll.clone();
        let ste = saw_exchange.clone();
        let sd = shutdown.clone();
        let url: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let url_clone = url.clone();

        let poll_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let pc = poll_count.clone();

        std::thread::spawn(move || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let addr = listener.local_addr().unwrap();
            *url_clone.lock().unwrap() = Some(format!("http://{addr}"));

            listener.set_nonblocking(true).expect("nonblocking");

            loop {
                if sd.load(Ordering::Relaxed) {
                    return;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        use std::io::{Read, Write};
                        let mut buf = [0; 4096];
                        let n = match stream.read(&mut buf) {
                            Ok(n) if n > 0 => n,
                            _ => continue,
                        };
                        let request = String::from_utf8_lossy(&buf[..n]);

                        let response = if request.contains("/api/accounts/deviceauth/usercode") {
                            si.store(true, Ordering::Relaxed);
                            json_response(
                                200,
                                r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"1"}"#,
                            )
                        } else if request.contains("/api/accounts/deviceauth/token") {
                            let count = pc.fetch_add(1, Ordering::Relaxed);
                            if count == 0 {
                                sp403.store(true, Ordering::Relaxed);
                                json_response(403, r#"{"error":"pending"}"#)
                            } else {
                                sp2.store(true, Ordering::Relaxed);
                                json_response(
                                    200,
                                    r#"{"authorization_code":"ac_2","code_verifier":"cv_2"}"#,
                                )
                            }
                        } else if request.contains("/oauth/token") {
                            ste.store(true, Ordering::Relaxed);
                            json_response(
                                200,
                                r#"{"access_token":"at_2","refresh_token":"rt_2","expires_in":3600}"#,
                            )
                        } else {
                            json_response(404, r#"{"error":"nf"}"#)
                        };

                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        std::thread::sleep(Duration::from_millis(100));
        let url = url.lock().unwrap().clone().unwrap();

        let sleeper = MockSleeper::default();
        let client = DeviceAuthClient {
            issuer: url,
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            sleeper: Box::new(sleeper),
            max_wait: Duration::from_secs(30),
        };
        let tokens = client.run().unwrap();
        assert_eq!(tokens.access_token, "at_2");
        assert!(saw_init.load(Ordering::Relaxed));
        assert!(saw_poll_403.load(Ordering::Relaxed));
        assert!(saw_second_poll.load(Ordering::Relaxed));
        assert!(saw_exchange.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_retries_404() {
        let saw_init = Arc::new(AtomicBool::new(false));
        let saw_poll_404 = Arc::new(AtomicBool::new(false));
        let saw_exchange = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let si = saw_init.clone();
        let sp404 = saw_poll_404.clone();
        let ste = saw_exchange.clone();
        let sd = shutdown.clone();
        let url: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let url_clone = url.clone();

        let poll_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let pc = poll_count.clone();

        std::thread::spawn(move || {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let addr = listener.local_addr().unwrap();
            *url_clone.lock().unwrap() = Some(format!("http://{addr}"));

            listener.set_nonblocking(true).expect("nonblocking");

            loop {
                if sd.load(Ordering::Relaxed) {
                    return;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        use std::io::{Read, Write};
                        let mut buf = [0; 4096];
                        let n = match stream.read(&mut buf) {
                            Ok(n) if n > 0 => n,
                            _ => continue,
                        };
                        let request = String::from_utf8_lossy(&buf[..n]);

                        let response = if request.contains("/api/accounts/deviceauth/usercode") {
                            si.store(true, Ordering::Relaxed);
                            json_response(
                                200,
                                r#"{"device_auth_id":"daid_1","user_code":"ABC-123","interval":"1"}"#,
                            )
                        } else if request.contains("/api/accounts/deviceauth/token") {
                            let count = pc.fetch_add(1, Ordering::Relaxed);
                            if count == 0 {
                                sp404.store(true, Ordering::Relaxed);
                                json_response(404, r#"{"error":"nf"}"#)
                            } else {
                                json_response(
                                    200,
                                    r#"{"authorization_code":"ac_3","code_verifier":"cv_3"}"#,
                                )
                            }
                        } else if request.contains("/oauth/token") {
                            ste.store(true, Ordering::Relaxed);
                            json_response(
                                200,
                                r#"{"access_token":"at_3","refresh_token":"rt_3","expires_in":3600}"#,
                            )
                        } else {
                            json_response(404, r#"{"error":"nf"}"#)
                        };

                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        std::thread::sleep(Duration::from_millis(100));
        let url = url.lock().unwrap().clone().unwrap();

        let sleeper = MockSleeper::default();
        let client = DeviceAuthClient {
            issuer: url,
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            sleeper: Box::new(sleeper),
            max_wait: Duration::from_secs(30),
        };
        let tokens = client.run().unwrap();
        assert_eq!(tokens.access_token, "at_3");
        assert!(saw_init.load(Ordering::Relaxed));
        assert!(saw_poll_404.load(Ordering::Relaxed));
        assert!(saw_exchange.load(Ordering::Relaxed));
    }

    #[test]
    fn device_flow_parse_interval() {
        assert_eq!(
            DeviceAuthClient::parse_interval("5"),
            Duration::from_millis(5000)
        );
        assert_eq!(
            DeviceAuthClient::parse_interval("0"),
            Duration::from_millis(5000)
        );
        assert_eq!(
            DeviceAuthClient::parse_interval("-1"),
            Duration::from_millis(1000)
        );
        assert_eq!(
            DeviceAuthClient::parse_interval("invalid"),
            Duration::from_millis(5000)
        );
        assert_eq!(
            DeviceAuthClient::parse_interval("10"),
            Duration::from_millis(10000)
        );
    }
}
