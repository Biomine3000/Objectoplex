*** Settings ***
Resource    single_server.robot
Library     common.py
Library     ObjectSystemConnection.py

Test Setup       Connect To Default Server
Test Teardown    Disconnect From Default Server


*** Test Cases ***
Subscription Without Natures
    ${subscription}=     Make Subscription Object
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}

Subscription With Natures
    ${first}=            Create List    foo          bar
    ${second}=           Create List    spam         ham
    ${subscription}=     Make Subscription Object    ${first}       ${second}
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}

Receive Object With Requested Nature
    ${natures}=               Create List    foo
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${natures}
    Send Object               ${obj}	
    Should Receive Object            ${obj}

*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe With Natures
    [Arguments]    ${natures}
    ${subscription}=     Make Subscription Object    ${natures}
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
