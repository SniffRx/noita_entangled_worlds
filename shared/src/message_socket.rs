use std::{
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    net::{SocketAddr, TcpStream},
    sync::mpsc::{self, RecvError, SyncSender, TryRecvError, TrySendError},
    thread::{self, JoinHandle},
    time::Duration,
};

use bitcode::{DecodeOwned, Encode};
use eyre::{Context, bail};
use tracing::info;

fn read_one<T: DecodeOwned>(mut buf: impl Read) -> eyre::Result<T> {
    let mut len_buf = [0u8; 4];
    buf.read_exact(&mut len_buf)
        .wrap_err("Couldn't receive the length from stream")?;
    let len = u32::from_le_bytes(len_buf);
    let mut out_buf = vec![0; usize::try_from(len)?];
    buf.read_exact(out_buf.as_mut_slice())
        .wrap_err("Couldn't read message body")?;
    bitcode::decode(&out_buf).wrap_err("Failed to decode message body")
}

pub struct MessageSocket<Inbound, Outbound> {
    send_messages: Option<SyncSender<Vec<u8>>>,
    shutdown_socket: TcpStream,
    recv_messages: mpsc::Receiver<eyre::Result<Inbound>>,
    reader_thread: Option<JoinHandle<()>>,
    writer_thread: Option<JoinHandle<()>>,
    _phantom: PhantomData<fn() -> Outbound>,
}

impl<Inbound: DecodeOwned + Send + 'static, Outbound: Encode> MessageSocket<Inbound, Outbound> {
    pub fn new(socket: TcpStream) -> eyre::Result<Self> {
        socket.set_write_timeout(Some(Duration::from_secs(10)))?;
        let (sender, recv_messages) = mpsc::channel();
        let (send_messages, outbound) = mpsc::sync_channel::<Vec<u8>>(4096);
        let reader_thread = Some(thread::spawn({
            let socket = socket.try_clone()?;
            move || {
                let mut socket = BufReader::new(socket);
                loop {
                    let res = read_one(&mut socket);
                    let res_was_error = res.is_err();
                    if sender.send(res).is_err() {
                        break;
                    }
                    if res_was_error {
                        break;
                    }
                }
            }
        }));
        let writer_thread = Some(thread::spawn({
            let socket = socket.try_clone()?;
            move || {
                let mut socket = BufWriter::new(socket);
                while let Ok(encoded) = outbound.recv() {
                    let Ok(len) = u32::try_from(encoded.len()) else {
                        break;
                    };
                    if socket.write_all(&u32::to_le_bytes(len)).is_err() {
                        break;
                    }
                    if socket.write_all(&encoded).is_err() {
                        break;
                    }
                    if socket.flush().is_err() {
                        break;
                    }
                }
            }
        }));

        Ok(Self {
            send_messages: Some(send_messages),
            shutdown_socket: socket,
            recv_messages,
            reader_thread,
            writer_thread,
            _phantom: PhantomData,
        })
    }

    pub fn connect(addr: &SocketAddr) -> eyre::Result<Self> {
        let stream = TcpStream::connect_timeout(addr, Duration::from_secs(1))?;
        Self::new(stream).wrap_err("Failed to wrap socket")
    }

    pub fn read(&mut self) -> eyre::Result<Inbound> {
        match self.recv_messages.recv() {
            Ok(msg) => msg,
            Err(RecvError) => bail!("Channel disconnected"),
        }
    }

    pub fn try_read(&mut self) -> eyre::Result<Option<Inbound>> {
        match self.recv_messages.try_recv() {
            Ok(msg) => Some(msg).transpose(),
            Err(TryRecvError::Disconnected) => bail!("Channel disconnected"),
            Err(TryRecvError::Empty) => Ok(None),
        }
    }

    pub fn write(&mut self, value: &Outbound) -> eyre::Result<()> {
        let encoded = bitcode::encode(value);
        u32::try_from(encoded.len()).wrap_err("Message too large to be sent")?;
        let Some(send_messages) = &self.send_messages else {
            bail!("Message socket writer disconnected");
        };
        match send_messages.try_send(encoded) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => bail!("Message socket outbound queue is full"),
            Err(TrySendError::Disconnected(_)) => bail!("Message socket writer disconnected"),
        }
        Ok(())
    }

    pub fn flush(&mut self) -> eyre::Result<()> {
        Ok(())
    }
}

impl<Inbound, Outbound> Drop for MessageSocket<Inbound, Outbound> {
    fn drop(&mut self) {
        self.shutdown_socket.shutdown(std::net::Shutdown::Both).ok();
        self.send_messages.take();
        if let Some(handle) = self.reader_thread.take() {
            handle.join().ok();
        }
        if let Some(handle) = self.writer_thread.take() {
            handle.join().ok();
        }
        info!("Message socket dropped");
    }
}
