# Heapfile and ingestion

It is very important to note that ingesting data and creating heap files depend on the config files.

This project has config files specifically because the assignment specified to not hardcode the paths.

Please change the config.property files to match your environment.

## Compiling Code

Also note that the ingestion files and the heapfiles get compiled outside the directory as:
`javac heapfile/*.java`
`javac ingestion/*.java`

This is because the classpaths are playing up on my AWS instance.

This also means that you have to execute the files from outside the directory aswell:
`java heapfile/dbload -p 4096 /home/ec2-user/5-million-rows.csv`
`java heapfile/dbquery 710 4096`
`java ingestion/Ingest derby`
`java ingestion/Ingest mongo`

Also a note, when it comes to the DataSplitter, when splitting the data for the mongo database,\
it uses a lot of memory, about 5GB worth of memory, so only run it on PC with a lot of memory.