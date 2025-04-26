# Exp1 dummy

Testing the waters.

Run two nodes. On node A, send a message. On node B, wait for that message.

If nodeB gets the message, the playbook succeeds.

If the timeout of 10s is reached, the playbook fails.

## PlantUML diagram

```plantuml
@startuml
participant "Node A" as A
participant "Node B" as B
hnote over A: signaling server
hnote over A: fledger (send)
hnote over B: fledger (recv)
rnote over A, B
  Setup with signaling server
endrnote
A -> B: chat message
rnote over A, B
  Timeout 10s
endrnote
rnote over B
  //exit code 0//
  //iff message received//
endrnote
@enduml
```
