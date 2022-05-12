use omnipaxos_core::{
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage},
    ballot_leader_election::{BallotLeaderElection, BLEConfig, messages::BLEMessage},
    messages::Message,
    util::LogEntry::{self, Decided},
};

use structopt::StructOpt;
use tokio::{
    net::{TcpListener, TcpStream},
    io::{AsyncReadExt, AsyncWriteExt, split},
    sync::mpsc::channel,
    sync::mpsc::Sender,
    sync::mpsc::Receiver,
};
use serde::{Serialize, Deserialize};
use std::{thread, time};    


/*
    A struct node that takes arguments from the command line
    A node has a pid and peers, pid needs to be a unique value
*/
#[derive(Debug, Serialize, Deserialize, StructOpt)]
struct Node {
    
    #[structopt(long)]
    pid: u64,
    
    #[structopt(long)]
    peers: Vec<u64>,
}
/*
    Log entries will consist of key-value pairs
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[tokio::main]
async fn main() {
    /*
        Create a node with pid and peers received from
        the command line
    */
    let node = Node::from_args();    
    let pid = node.pid;
    let peers = node.peers;
    
    /*
        Create a SequencePaxos and BLE struct
        Code from The OmniPaxos Book
    */
    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(pid.try_into().unwrap());
    sp_config.set_pid(pid);
    sp_config.set_peers(peers.to_vec());
    
    let storage = MemoryStorage::<KeyValue, ()>::default();
    let sp = SequencePaxos::with(sp_config, storage);

    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(pid);
    ble_config.set_peers(peers.to_vec());
    //leader timeout of 20 ticks
    ble_config.set_hb_delay(20); 
    
    let ble = BallotLeaderElection::with(ble_config);
    
    /*
        Create a SP and BLE channel
        All data sent on sender will become available
        on receiver
    */
    //SP
    let (sender1, receiver) = channel(64);
    let sender2 = sender1.clone();
    let sender3 = sender1.clone();
    let sender4 = sender1.clone();

    //BLE
    let (sender5, receiver2) = channel(64);
    let sender6 = sender5.clone();
    let sender7 = sender5.clone();

    /*
        Spawn tasks/threads to handle SequencePaxos,
        BLE, and command line stuff.
    */

    tokio::spawn(async move {
        sp_handler(sp, receiver).await;
    });
    tokio::spawn(async move {
        ble_handler(ble, receiver2, sender1).await;
    });

    tokio::spawn(async move {
        out_msgs(sender2, sender5).await;
    });

    tokio::spawn(async move {
        client_handler(sender3, &pid).await;
    });

    tokio::spawn(async move {
        ble_timer(sender6).await;
    });

    tokio::spawn(async move {
        incoming_ble(sender7, &pid).await;
    });

    /*
        Send number of peers to client when created
        Used for distributing key-value pairs over the nodes
    */
    let peers_length: u64 = peers.len().try_into().unwrap();
    let stream = TcpStream::connect(format!("127.0.0.1:64999")).await.unwrap();
    let (_, mut writehalf) = split(stream);
    //let mut peers_vec: Vec<&u64> = Vec::new();
    //peers_vec.push(&peers_length);
    //peers_vec.push(&pid);
    //let msg: Vec<u8> = bincode::serialize(&peers_vec).unwrap();
    let msg: Vec<u8> = bincode::serialize(&peers_length).unwrap();
    writehalf.write_all(&msg).await.unwrap();
    println!("Node {} sent number of peers to client", pid);
    
    let addr: String = "127.0.0.1:".to_owned();
    let address: String = format!("{}{}", addr, 55000+pid);
    let socket_address = TcpListener::bind(address).await.unwrap();

    loop {
        let sender8 = sender4.clone();
        let (socket, _) = socket_address.accept().await.unwrap();
        
        tokio::spawn(async move {
            handle_incoming_msgs(socket, sender8).await;
        });
    }
}

/*
    When a message is received from the network layer, handle
    it in Sequence Paxos.
*/
async fn handle_incoming_msgs(socket: TcpStream, sd: Sender<(&str, Vec<u8>)>) {
    
    let (mut rd, _) = split(socket);
    let mut buf = vec![0; 1024];

    loop {
        let bytes = rd.read(&mut buf).await.unwrap();
        
        if bytes == 0 {
            break;
        }

        sd.send(("incoming", (&buf[..bytes]).to_vec())).await.unwrap();
    }
}
/*
    Send outgoing messages to receiver on network layer
*/
async fn out_msgs(sd_sp: Sender<(&str, Vec<u8>)>,
                                         sd_ble: Sender<(&str, Vec<u8>)>) {
    loop {
        thread::sleep(time::Duration::from_millis(1));
        sd_sp.send(("out_msgs", vec![])).await.unwrap();
        sd_ble.send(("out_msgs", vec![])).await.unwrap();
    }
}

/*
    Handle all ble stuff
*/

async fn ble_timer(sd: Sender<(&str, Vec<u8>)>) {
    loop {
        thread::sleep(time::Duration::from_millis(20));
        sd.send(("leader_timeout", vec![])).await.unwrap();
    }
}
 
async fn incoming_ble(sd: Sender<(&str, Vec<u8>)>, pid: &u64) {
    let addr: String = "127.0.0.1:".to_owned();
    let address: String = format!("{}{}", addr, 61000+pid);
    
    let listener = TcpListener::bind(address).await.unwrap();
    
    loop {
        let (socket, _) = listener.accept().await.unwrap();    
        let (mut readhalf, _) = split(socket);
        let mut buf = vec![0; 1024];
        loop {
            let bytes = readhalf.read(&mut buf).await.unwrap();
            
            if bytes == 0 {
                break;
            }
            sd.send(("ble_handle", (&buf[..bytes]).to_vec())).await.unwrap();
        }
    }
}
async fn ble_handler(mut ble: BallotLeaderElection,
                     mut receiver: Receiver<(&str, Vec<u8>)>,
                     sender: Sender<(&str, Vec<u8>)>) {
    while let Some(message) = receiver.recv().await {
        match (message.0, message.1) {
            //handle incoming messages from network layer
            ("ble_handle", msg_enc) => {
                let msg_dec: BLEMessage = bincode::deserialize(&msg_enc).unwrap();
                ble.handle(msg_dec);
            },
            //send outgoing messages periodically
            ("out_msgs", _message) => {
                for out_msg in ble.get_outgoing_msgs() {
                    let receiver = out_msg.to;
                    let addr: String = "127.0.0.1:".to_owned();
                    let address: String = format!("{}{}", addr, 61000+receiver);
                    match TcpStream::connect(address).await {
                        Ok(stream) => {
                        // send ble messages
                            let (_, mut writehalf) = split(stream);
                            
                            let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                            writehalf.write_all(&msg_enc).await.unwrap();
                        },
                        Err(e) => println!("{}", e),
                    }
                }
            },
            ("leader_timeout", _message) => {
                if let Some(leader) = ble.tick() {
                    let msg_enc: Vec<u8> = bincode::serialize(&leader).unwrap();
                    sender.send(("handle_leader", msg_enc)).await.unwrap();
                }
            },
            _ => {
                println!("Did not match with any message in ble_handler");
            }
        }
    }
}
/*
    Handle all sp stuff
*/
async fn sp_handler(mut seq_paxos: SequencePaxos<KeyValue, (), MemoryStorage<KeyValue, ()>>, 
                    mut receiver: Receiver<(&str, Vec<u8>)>) {
    while let Some(message) = receiver.recv().await {
        match (message.0, message.1) {
            //handle incoming message from network layer
            ("incoming", msg_enc) => {
                let msg: Message<KeyValue, ()> = bincode::deserialize(&msg_enc).unwrap();
                seq_paxos.handle(msg);
            },
            //read the kv entry with the specific key from the log
            ("get", getmsg) => {
                let key: String = bincode::deserialize(&getmsg).unwrap();
                //read decided entries from index 0
                let decided: Option<Vec<LogEntry<KeyValue, ()>>> = seq_paxos.read_decided_suffix(0);
                let stream1 = TcpStream::connect("127.0.0.1:63000").await.unwrap();
                let (_, mut writehalf1) = split(stream1);

                match decided {
                    Some(entry) => {
                        let logentry: Vec<LogEntry<'_, KeyValue, ()>> = entry.to_vec();
                        
                        let mut kvpair: String = "".to_string();
                        let mut i = 0;
                        //read decided entries 
                        loop {
                            match logentry[i] {
                                Decided(kv_pair) => {
                                    if kv_pair.key == key {
                                        kvpair.push_str(&format!("{} {}", kv_pair.key, kv_pair.value));
                                        break;
                                    }
                                },
                                _ => println!("Have not find key-value pair yet"),
                            };
                            if i == logentry.len()-1{
                                break;
                            }
                            i+=1;
                        }
                        if kvpair.len() <= 1 {
                            kvpair = "Key does not exist".to_string();
                        }
                        let stream2 = TcpStream::connect("127.0.0.1:63000").await.unwrap();
                        let (_, mut writehalf2) = split(stream2); 
                        let ser_kvpair: Vec<u8> = bincode::serialize(&kvpair).unwrap();
                        writehalf2.write_all(&ser_kvpair).await.unwrap();
                    },
                    _ => {
                        let no_key: Vec<u8> = bincode::serialize("Key does not exist").unwrap();
                        writehalf1.write_all(&no_key).await.unwrap();
                    },
                }
            },
            ("put", putmsg) => {
                let put_entry: KeyValue = bincode::deserialize(&putmsg).unwrap();
                //append kv entry to the replicated log
                seq_paxos.append(put_entry).expect("Failed to append");

            },
            ("handle_leader", message) => {
                seq_paxos.handle_leader(bincode::deserialize(&message).unwrap());
            },
            ("out_msgs", _message) => {
                //send outgoing messages. This should be called periodically
                for out_msg in seq_paxos.get_outgoing_msgs() {
                    let receiver = out_msg.to;
                    let addr: String = "127.0.0.1:".to_owned();
                    let address: String = format!("{}{}", addr, 55000+receiver);
                    match TcpStream::connect(address).await{
                        Ok(stream) => {
                            let (_, mut writehalf) = split(stream);
                            let msg_enc: Vec<u8> = bincode::serialize(&out_msg).unwrap();
                            writehalf.write_all(&msg_enc).await.unwrap();
                        },
                        Err(e) => println!("{}", e),
                    }
                }
            }
            _ => {
                println!("Did not match with any message in sp_handler");
            }
        }
    }                
}

/*
    Handle messages (get and put) received from client
*/

async fn client_handler(sd: Sender<(&str, Vec<u8>)>, pid: &u64) {

    //create a TCPListener to the socket address
    //to receive messages from client. 
    let addr: String = "127.0.0.1:".to_owned();
    let address: String = format!("{}{}", addr, 63000+pid);
    let listener = TcpListener::bind(address).await.unwrap();

    loop {
        //accept tcp connections
        let (socket, _) = listener.accept().await.unwrap();
        let (mut readhalf, _) = split(socket);
        let mut buf = vec![0; 1024];
        loop {
            let bytes = readhalf.read(&mut buf).await.unwrap();
            if bytes == 0 {
                break;
            }

            let message: String = bincode::deserialize(&buf[..bytes]).unwrap();
            let v:Vec<&str> = message.split(' ').collect();
            let key: String = String::from(v[1].to_string());
            match v[0] {
                "put" => {
                    let value: u64 = v[2].parse().expect("failed to get value");
                    let kv_pair = KeyValue{key: key, value: value};

                    sd.send(("put", bincode::serialize(&kv_pair).unwrap())).await.unwrap();
                },
                "get" => {
                    //sd.send(("get", bincode::serialize(&String::from(v[1])).unwrap())).await.unwrap();
                    sd.send(("get", bincode::serialize(&key).unwrap())).await.unwrap();
                },
                _ => println!("message did not match"),
    
            };
        }
    }
}