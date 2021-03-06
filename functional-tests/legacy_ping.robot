*** Settings ***
Resource    single_server.robot
Library     common.py
Library     ObjectSystemConnection.py

Test Setup       Connect To Default Server
Test Teardown    Disconnect From Default Server

*** Test Cases ***
Should Respond To Ping
    [Tags]    server    ping    legacy
    Subscribe
    ${ping}=                             Make Event           ping
    Send Object                          ${ping}
    ${pong}=                             Receive Reply For    ${ping}
    Object Should Have Key With Value    ${pong}              event    pong

Should Not Respond To Ping Before Subscription
    [Tags]    server    ping    legacy
    ${ping}=                        Make Event    ping
    Send Object                     ${ping}
    Should Not Receive Reply For    ${ping}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe
    ${subscription}=     Make Legacy Subscription Object
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
