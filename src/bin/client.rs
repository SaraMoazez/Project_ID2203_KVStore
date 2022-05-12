use tokio::{
    net::{TcpListener, TcpStream},
    io::{AsyncReadExt, AsyncWriteExt, split},
    sync::mpsc::channel,
    sync::mpsc,
};

//use serde::{Serialize, Deserialize};

#[tokio::main]
async fn main() {
    
    use std::io;

    let (sender1, receiver) = channel(64);
    let sender2 = sender1.clone();

    tokio::spawn(async move {
        receive(sender1).await;
    }); 
    tokio::spawn(async move {
        handle_msgs(receiver).await;
    });
    tokio::spawn(async move {
        key_value_to_cmd().await;
    });

    println!("Please enter a command");
    loop {
        
        let mut command = String::new();
        io::stdin().read_line(&mut command).expect("Could not read command");

        let v:Vec<&str> = command.split(' ').collect();
           
        if v[0] == "put" {
            sender2.send(("put", bincode::serialize(&command).unwrap())).await.unwrap();
        }
        else if v[0] == "get" {
            sender2.send(("get", bincode::serialize(&command).unwrap())).await.unwrap();
        }
        else{
            println!("Please enter a valid command");
        }
    }
}
/*
    Handle messages received from the command line
*/

async fn handle_msgs(mut receiver: mpsc::Receiver<(&str, Vec<u8>)>) {
    let mut peers: u64 = 0; 
    while let Some(message) = receiver.recv().await {
        match (message.0, message.1) {
            ("peers", new_peers) => {
                let peers_new: u64 = bincode::deserialize(&new_peers).unwrap();
                peers = peers_new;
            },

            ("put", putmsg) => {
                let mut destination = 0;

                let des_putmsg: String = bincode::deserialize(&putmsg).unwrap();
                let v:Vec<&str> = des_putmsg.split(' ').collect();
                let key: u64 = v[1].parse().expect("Key needs to be a number");

                if v.len() == 2 {
                    println!("Error: put message needs a value");
                }
                else{
                    let _value: u64 = v[2].trim().parse().expect("Value needs to be a number");
                    //distribute the key value pair to the correct node
                    for i in 0..key {
                        if i % 5 == 0{
                            destination = destination + 1;
                        }
                        if i == key{
                            break;
                        }
                        if destination >= peers{
                            break;
                        }
                    } 
                    let addr: String = "127.0.0.1:".to_owned();
                    let address: String = format!("{}{}", addr, 63000+destination);
            
                    println!("Sending put request to node: {}", destination);
            
                    let stream = TcpStream::connect(address).await.unwrap();
                    let (_, mut writehalf) = split(stream);    

                    let message: Vec<u8> = bincode::serialize(&des_putmsg.trim()).unwrap();
                    writehalf.write_all(&message).await.unwrap();
                }
            },

            ("get", getmsg) => {
                let mut destination = 0;


                let des_getmsg: String = bincode::deserialize(&getmsg).unwrap();
                let v:Vec<&str> = des_getmsg.split(' ').collect();
                let key: u64 = v[1].trim().parse().expect("Key needs to be a number");
                //send get message to the correct node
                for i in 0..key {
                    if i % 5 == 0{
                        destination = destination + 1;
                    }
                    if i == key{
                        break;
                    }
                    if destination >= peers{
                        break;
                    }
                }

                let addr: String = "127.0.0.1:".to_owned();
                let address: String = format!("{}{}", addr, 63000+destination);
        
                println!("Sending get request to node: {}", destination);
        
                let stream = TcpStream::connect(address).await.unwrap();
                let (_, mut writehalf) = split(stream);    

                let msg: Vec<u8> = bincode::serialize(&des_getmsg.trim()).unwrap();
                writehalf.write_all(&msg).await.unwrap();
            },
            _ => {
                println!("Did not match with any message");
            },
        }
    }
}
/*
    Print key value pair to the command line
*/
async fn key_value_to_cmd() {
    let listener = TcpListener::bind("127.0.0.1:63000").await.unwrap();
    loop {
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
            if v[0] == "Key" {
                println!("Key does not exist");
            }
            else {
                println!("Key: {} Value: {}", v[0], v[1]);
            }   
        }
    }
}
/*
    Receive peers message from nodes
*/
async fn receive(sd: mpsc::Sender<(&str, Vec<u8>)>) {
    let listener = TcpListener::bind("127.0.0.1:64999").await.unwrap();

    loop{
        let (socket, _) = listener.accept().await.unwrap();
        let (mut readhalf, _) = split(socket);
        let mut buf = [0; 1024];

        loop{
            let n = readhalf.read(&mut buf).await.unwrap();
            match n{
                0 => break,
                bytes => {
                    //let deserialized_message: Vec<u64> = bincode::deserialize(&buf[0..bytes]).unwrap();
                    let deserialized_message: u64 = bincode::deserialize(&buf[0..bytes]).unwrap();
                    //println!("Client received number of peers from node: {}", deserialized_message[1]);
                    sd.send(("peers", bincode::serialize(&(deserialized_message + 1)).unwrap())).await.unwrap();
                    //sd.send(("peers", bincode::serialize(&(deserialized_message[0] +1)).unwrap())).await.unwrap();
                    break;
                },
            };
        }
    }
} 

