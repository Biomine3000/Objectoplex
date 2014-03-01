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
    ${natures}=          Set Natures    foo&bar|baz
    ${subscription}=     Make Subscription Object    ${natures}
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}


# Single nature
Receive Object With Requested Nature
    ${natures}=               Set Natures    foo
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${natures}
    Send Object               ${obj}
    Should Receive Object     ${obj}

Shouldn't Receive Object Without Requested Nature
    ${natures}=                   Set Natures    foo
    ${obj_natures}=               Set Natures    bar
    Subscribe With Natures        ${natures}
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}


# Multiple natures
Receive Object With Multiple Requested Natures
    ${natures}=               Set Natures    foo&bar
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${natures}
    Send Object               ${obj}
    Should Receive Object     ${obj}

Shouldn't Receive Object Without Multiple Requested Natures
    ${natures}=                   Set Natures    foo&bar
    ${obj_natures}=               Set Natures    spam&ham
    Subscribe With Natures        ${natures}
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}


# Subsets
Should Receive Object With Subset Of Requested Natures
    ${natures}=               Set Natures    foo&bar
    ${obj_natures}=           Set Natures    foo&bar&baz
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${obj_natures}
    Send Object               ${obj}
    Should Receive Object     ${obj}

# Or
Should Receive When One Nature Set Matches
    ${natures}=               Set Natures    foo&bar|spam&ham|xyz&abc
    ${obj_natures}=           Set Natures    foo&bar
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${obj_natures}
    Send Object               ${obj}
    Should Receive Object     ${obj}

Should Receive When Multiple Nature Sets Match
    ${natures}=               Set Natures    foo&bar|spam&ham|xyz&abc
    ${obj_natures}=           Set Natures    xyz&foo&abc&bar
    Subscribe With Natures    ${natures}
    ${obj}=                   Make Object With Natures    ${obj_natures}
    Send Object               ${obj}
    Should Receive Object     ${obj}

Should Treat Distinct Sets Correctly
    ${natures}=                  Set Natures    foo&bar|xyz&abc
    ${obj_natures}=              Set Natures    foo&abc
    Subscribe With Natures       ${natures}
    ${obj}=                      Make Object With Natures    ${obj_natures}
    Send Object                  ${obj}
    Should Not Receive Object    ${obj}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe With Natures
    [Arguments]          ${natures}
    ${subscription}=     Make Subscription Object    ${natures}
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}
