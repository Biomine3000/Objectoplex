*** Settings ***
Resource    single_server.robot
Library     common.py
Library     ObjectSystemConnection.py
Library     client_registry.py

Test Setup       Connect To Default Server
Test Teardown    Disconnect From Default Server


*** Test Cases ***
Client Registry Service Should Reply
    [Tags]    services    client_registry
    Subscribe
    ${obj}=                     Make Join Request
    Send Object                 ${obj}
    Should Receive Reply For    ${obj}

Reply Should Contain The Client In Client List
    [Tags]    services    client_registry    wip
    ${sub_reply}=                       Subscribe
    ${join}=                            Make Join Request
    Send Object                         ${join}
    Should Receive Reply For            ${join}
    ${call}=                            Make List Request
    Send Object                         ${call}
    ${reply}=                           Receive Reply For                      ${call}
    Should Reply With Correct Object    ${sub_reply.metadata['routing-id']}    ${join}    ${reply}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe
    [Return]    ${reply}
    ${subscription}=     Make Subscription Object
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
