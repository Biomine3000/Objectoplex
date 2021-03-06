# -*- coding: utf-8 -*-
"""
As per https://gist.githubusercontent.com/arkku/4afdfce6b6199fb0193f/raw/f9699b2c4a448d74ee6e95dc15c562b2a7af497a/subscription+rules+pseudocode

function message_passed_by_subscription_rules(msg, subscription_rules)
  pass := false
  for rule in subscription_rules do
    if is_negative_rule := rule.has_prefix('!') then
      rule := rule.without_first_character
    end

    if rule.has_prefix('#') then
      rule := rule.without_first_character
      for nature in msg.natures do
        if rule.matches(nature) then
          pass := not is_negative_rule
          break
        end
      end
    elseif rule.has_prefix('@') then
      rule := rule.without_first_character
      if msg.is_an_event and rule.matches(msg.event_type) then
        pass := not is_negative_rule
      end
    elseif rule.matches(msg.type) then
      # empty/null type must match * wildcard
      pass := not is_negative_rule
    end
  end
  return pass
end
"""

def match(matcher, matchable):
    if matcher is None or matchable is None:
        return False

    matcher_parts = matcher.split('/')
    matchable_parts = matchable.split('/')

    for index, matcher_part in enumerate(matcher_parts):
        if matcher_part == '*':
            return True

        if index >= len(matchable_parts):
            return False
        matchable_part = matchable_parts[index]
        if matcher_part != matchable_part:
            return False

    return True


def routing_decision(message, rules):
    PASS = False # pass written in lowercase is a keyword in Python

    for rule in rules:
        is_negative_rule = rule.startswith('!')
        if is_negative_rule:
            rule = rule[1:]

        if rule.startswith('#'):
            rule = rule[1:]
            for nature in message.metadata.get('natures', []):
                if match(rule, nature): # nature == rule:
                    PASS = not is_negative_rule
                    break

        elif rule.startswith('@'):
            rule = rule[1:]
            if message.event is not None and match(rule, message.event): # message.event == rule:
                PASS = not is_negative_rule

        elif rule == '*' or match(rule, message.metadata.get('type', None)):
            PASS = not is_negative_rule

    return PASS
