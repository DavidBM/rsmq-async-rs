use std::fs;
use std::process;
use std::thread::sleep;
use std::time::Duration;

pub struct RedisServer {
    pub process: Option<process::Child>,
    addr: redis::ConnectionAddr,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        if which_redis_server() {
            let addr = {
                let listener = net2::TcpBuilder::new_v4()
                    .unwrap()
                    .reuse_address(true)
                    .unwrap()
                    .bind("127.0.0.1:0")
                    .unwrap()
                    .listen(1)
                    .unwrap();
                let server_port = listener.local_addr().unwrap().port();
                redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), server_port)
            };
            RedisServer::spawn(addr)
        } else {
            let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
            let (host, port) = url
                .rsplit_once(':')
                .map(|(h, p)| (h.to_string(), p.parse().expect("invalid REDIS_URL port")))
                .unwrap_or_else(|| (url, 6379));
            RedisServer {
                process: None,
                addr: redis::ConnectionAddr::Tcp(host, port),
            }
        }
    }

    fn spawn(addr: redis::ConnectionAddr) -> RedisServer {
        let mut cmd = process::Command::new("redis-server");
        cmd.stdout(process::Stdio::null())
            .stderr(process::Stdio::null());

        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                cmd.arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);
            }
            redis::ConnectionAddr::Unix(ref path) => {
                cmd.arg("--port").arg("0").arg("--unixsocket").arg(path);
            }
            _ => panic!("No TLS support for the tests"),
        };

        RedisServer {
            process: Some(cmd.spawn().expect("Error executing redis-server")),
            addr,
        }
    }

    pub fn get_client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn stop(&mut self) {
        if let Some(ref mut child) = self.process {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let redis::ConnectionAddr::Unix(ref path) = *self.get_client_addr() {
            fs::remove_file(path).ok();
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

fn which_redis_server() -> bool {
    std::process::Command::new("redis-server")
        .arg("--version")
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .is_ok()
}

pub struct TestContext {
    #[allow(dead_code)]
    pub server: RedisServer,
    pub client: redis::Client,
}

impl TestContext {
    pub fn new() -> TestContext {
        let server = RedisServer::new();

        let conn_info = "redis://localhost"
            .parse::<redis::ConnectionInfo>()
            .unwrap()
            .set_addr(server.get_client_addr().clone());
        let client = redis::Client::open(conn_info).unwrap();
        let mut con;

        let millisecond = Duration::from_millis(1);
        loop {
            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        sleep(millisecond);
                    } else {
                        panic!("Could not connect: {}", err);
                    }
                }
                Ok(x) => {
                    con = x;
                    break;
                }
            }
        }
        let _ = redis::cmd("FLUSHDB").exec(&mut con);

        TestContext { server, client }
    }

    pub async fn async_connection(&self) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }
}
