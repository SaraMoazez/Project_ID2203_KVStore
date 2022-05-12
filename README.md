# Project_ID2203_KVStore
Project for the course ID2203 Distributed Systems, Advanced Course at KTH. A replicated distributed key-value store that supports the operations PUT and GET is implemented. 

# How to run

1. Start the client: To start the client, the following command should be used:  `cargo run --bin client`
2. Create nodes: To create a node, open a new command prompt and use the command:

  `cargo run --bin id_2203 -- --pid [pid number] --peers [pids of peers]`

It is also possible to run by using the command run_all.bat. This command will start the client and three nodes automatically. 

# PUT and GET
To insert a key-value pair to the key-value store the command `put key value` should be used. To retrieve a value associated with a key, the command `get key` should be used.

# Example
An example that shows how to start the client and four nodes as well as how to interact with the key-value store are showed below. 

`cargo run --bin client`

`cargo run --bin id_2203 -- --pid 1 --peers 2 3 4`

`cargo run --bin id_2203 -- --pid 2 --peers 1 3 4`

`cargo run --bin id_2203 -- --pid 3 --peers 1 2 4`

`cargo run --bin id_2203 -- --pid 4 --peers 1 2 3`

`put 3 4`

`get 3`



