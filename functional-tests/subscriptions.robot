*** Settings ***
Resource    single_server.robot
Library     common.py
Library     ObjectSystemConnection.py

Test Setup       Connect To Default Server
Test Teardown    Disconnect From Default Server


# The array of subscription rules is an ordered list. The last matching rule in
# the list determines pass/no pass. If no rules match (or the list is empty),
# the default is not to pass.

#     Each rule is a string
#     A ! prefix on a rule (before the type prefix, if any) negates it (i.e., don't pass if it matches)
#     Rules prefixed with # apply to natures, @ to events, and without a prefix to types
#     Rules may end in a star *, which is a wildcard matching any number of characters
#     A lone star * matches everything, including events (i.e., type defaults to the empty string)
#     Implementations may support more general wildcards at their discretion

# Example subscriptions:

#     nothing: [ ]
#     everything: [ * ]
#     events only: [ @* ]
#     not events: [ *, !@* ]
#     images and events: [ image/*, @* ]
#     images not having nature hasselhoff, and events (even with hasselhoff nature): [ image/*, !#hasselhoff, @* ]
#     images and events, as long as neither of them has hasselhoff nature: [ image/*, @*, !#hasselhoff ]
#     everything except images, unless they have hasselhoff nature: [ *, !image/*, #hasselhoff ]


*** Test Cases ***
Nothing
    [Tags]    server    rules
    ${subscription}=              Make Subscription Object
    Send Object                   ${subscription}
    ${reply}=                     Receive Reply For    ${subscription}

    ${obj_natures}=               Set Natures                 hasselhoff
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}

    ${event_obj}=                 Make Event           generic/event
    Send Object                   ${event_obj}
    Should Not Receive Object     ${event_obj}

All
    [Tags]    server    rules
    ${rules}=                   Parse Rules                 *
    Subscribe With Rules        ${rules}

    ${obj_natures}=             Set Natures                 hasselhoff
    ${obj}=                     Make Object With Natures    ${obj_natures}
    Send Object                 ${obj}
    Should Receive Object       ${obj}


Only Events
    [Tags]    server    rules
    Subscribe                     @*

    ${obj_natures}=               Set Natures                 hasselhoff
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}

    ${event_obj}=                 Make Event                  generic/event
    Send Object                   ${event_obj}
    Should Receive Object         ${event_obj}


Everything But Events
    [Tags]    server    rules
    Subscribe                     *, !@*

    ${obj_natures}=               Set Natures                 hasselhoff
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Receive Object         ${obj}

    ${event_obj}=                 Make Event                  generic/event
    Send Object                   ${event_obj}
    Should Not Receive Object     ${event_obj}


Texts And Events
    [Tags]    server    rules
    Subscribe                     text/*, @*

    ${obj_natures}=               Set Natures                 hasselhoff
    ${obj}=                       Make Object With Natures    ${obj_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}

    ${app_obj}=                   Make Application Object     gonzo
    Send Object                   ${app_obj}
    Should Not Receive Object     ${app_obj}

    ${event_obj}=                 Make Event                  generic/event
    Send Object                   ${event_obj}
    Should Receive Object         ${event_obj}

    ${text_obj}=                  Make Text Object            gonzo
    Send Object                   ${text_obj}
    Should Receive Object         ${text_obj}


Texts Not Having Hasselhoff Nature And Events With Hasselhoff Nature
    [Tags]    server    rules
    Subscribe                     text/*, !\#hasselhoff, @*

    ${hoff_natures}=              Set Natures                 hasselhoff

    ${obj}=                       Make Text Object            gonzo            ${hoff_natures}
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}

    ${event_obj1}=                Make Event                  generic/event    ${hoff_natures}
    Send Object                   ${event_obj1}
    Should Receive Object         ${event_obj1}

    ${obj}=                       Make Text Object            gonzo
    Send Object                   ${obj}
    Should Receive Object         ${obj}

    ${event_obj2}=                Make Event                  generic/event
    Send Object                   ${event_obj2}
    Should Receive Object         ${event_obj2}


Texts And Events Without Hasselhoff Natures
    [Tags]    server    rules
    Subscribe                     text/*, @*, !\#hasselhoff

    ${hoff_natures}=              Set Natures                 hasselhoff
    ${obj1}=                      Make Text Object            gonzo            ${hoff_natures}
    Send Object                   ${obj1}
    Should Not Receive Object     ${obj1}
    ${event_obj1}=                Make Event                  generic/event    ${hoff_natures}
    Send Object                   ${event_obj1}
    Should Not Receive Object     ${event_obj1}

    ${obj2}=                      Make Text Object            gonzo
    Send Object                   ${obj2}
    Should Receive Object         ${obj2}
    ${event_obj2}=                Make Event                  generic/event
    Send Object                   ${event_obj2}
    Should Receive Object         ${event_obj2}


Everything Except Texts Unless They Have Hasselhoff Nature
    [Tags]    server    rules
    Subscribe                     *, !text/*, \#hasselhoff

    ${hoff_natures}=              Set Natures                 hasselhoff

    ${app_obj}=                   Make Application Object     gonzo
    Send Object                   ${app_obj}
    Should Receive Object         ${app_obj}

    ${obj}=                       Make Text Object            gonzo
    Send Object                   ${obj}
    Should Not Receive Object     ${obj}

    ${obj}=                       Make Text Object            gonzo            ${hoff_natures}
    Send Object                   ${obj}
    Should Receive Object         ${obj}


*** Keywords ***
Connect To Default Server
    Connect To Server    ${SERVER HOST}    ${SERVER PORT}

Disconnect From Default Server
    Disconnect From Server

Subscribe With Rules
    [Arguments]          ${rules}
    ${subscription}=     Make Subscription Object    ${rules}
    Send Object          ${subscription}
    ${reply}=            Receive Reply For    ${subscription}

Subscribe
    [Arguments]          ${raw_rules}
    ${rules}=    Parse Rules    ${raw_rules}
    Subscribe With Rules        ${raw_rules}