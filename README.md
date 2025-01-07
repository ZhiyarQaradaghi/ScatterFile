ScatterFile is a distributed system application that processes input files (text, image, or other formats) by converting them into byte arrays and distributing the chunks to multiple processes for handling. It operates in two parts:

1- Pub-Sub Topology: The main process acts as a publisher, sending data chunks to all subscribed processes simultaneously.<br>
2- Push-Pull with Ring Topology: The main process pushes chunks to the first process, which forwards them sequentially through a ring of processes. The last process sends the processed data back to the main process.

Both parts utilize ZeroMQ for communication, ensuring efficient and reliable data transfer throughout the system. Finally, the main process reconstructs the original file seamlessly from the received chunks.
