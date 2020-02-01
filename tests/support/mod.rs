use net2;
use redis;
use std::fs;
use std::process;
use std::thread::sleep;
use std::time::Duration;

pub struct RedisServer {
    pub process: process::Child,
    addr: redis::ConnectionAddr,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        let addr = {
            // this is technically a race but we can't do better with
            // the tools that redis gives us :(
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
        RedisServer::new_with_addr(addr, |cmd| cmd.spawn().unwrap())
    }

    pub fn new_with_addr<F: FnOnce(&mut process::Command) -> process::Child>(
        addr: redis::ConnectionAddr,
        spawner: F,
    ) -> RedisServer {
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
                cmd.arg("--port").arg("0").arg("--unixsocket").arg(&path);
            }
        };

        RedisServer {
            process: spawner(&mut cmd),
            addr,
        }
    }

    pub fn get_client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = *self.get_client_addr() {
            fs::remove_file(&path).ok();
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
}

impl TestContext {
    pub fn new() -> TestContext {
        let server = RedisServer::new();

        let client = redis::Client::open(redis::ConnectionInfo {
            addr: Box::new(server.get_client_addr().clone()),
            db: 0,
            passwd: None,
        })
        .unwrap();
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
        redis::cmd("FLUSHDB").execute(&mut con);

        TestContext { server, client }
    }

    pub async fn async_connection(&self) -> redis::RedisResult<redis::aio::Connection> {
        self.client.get_async_connection().await
    }

}
