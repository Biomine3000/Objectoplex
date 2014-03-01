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
    [Tags]    services    client_registry
    Subscribe
    ${obj}=                             Make Join Request
    Send Object                         ${obj}
    Should Receive Reply For            ${obj}
    ${call}=                            Make List Request
    ${reply}=                           Receive Reply For    ${call}
    Should Reply With Correct Object    ${call}              ${reply}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe
    ${subscription}=     Make Subscription Object
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
