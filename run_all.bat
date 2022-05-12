
::Start the client
start cmd /k cargo run --bin client

::wait for 4 seconds before starting the nodes
timeout /t 4

::create nodes, each node need to have an unique pid.
start cmd /k cargo run --bin id_2203 -- --pid 1 --peers 2 3
start cmd /k cargo run --bin id_2203 -- --pid 2 --peers 1 3
start cmd /k cargo run --bin id_2203 -- --pid 3 --peers 1 2
::start cmd /k cargo run --bin iD2203 -- --pid 4 --peers 1 2 3 5
::start cmd /k cargo run --bin iD2203 -- --pid 5 --peers 1 2 3 4

cls
