*** Settings ***
Resource    single_server.robot
Library     common.py
Library     ObjectSystemConnection.py

Test Setup       Connect To Default Server
Test Teardown    Disconnect From Default Server

*** Test Cases ***
Should Respond To Ping
    Subscribe
    ${ping}=                             Make Ping Object
    Send Object                          ${ping}
    ${pong}=                             Receive Reply For    ${ping}
    Object Should Have Key With Value    ${pong}              event    pong

Should Not Respond To Ping Before Subscription
    ${ping}=                        Make Ping Object
    Should Not Receive Reply For    ${ping}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe
    ${subscription}=     Make Subscription Object
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
