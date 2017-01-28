import termcolor


def __format_highlight(text, match, color, add_keys=None):
    if add_keys is None:
        return termcolor.colored(text, color)
    else:
        match_info = [
            (str(k), str(v)) for k, v in sorted(match.items()) if k in add_keys
        ]
        output = '[{} [{}]]'.format(
            text, ', '.join('{}:{}'.format(*e) for e in match_info)
        )
        return termcolor.colored(output, color)


def highlight_matches(text, matches, color='magenta', match_info=None):
    """Highlight matches in a different color"""
    if match_info is None:
        match_info = {}
    else:
        match_info = set(match_info)

    matches = sorted(matches, key=lambda m: (m['start'], -m['end']))

    last_pos = 0
    output_text_fragments = []

    for match in matches:
        start, end = match['start'], match['end']
        if last_pos > start:
            continue

        output_text_fragments.append(text[last_pos:start])
        output_text_fragments.append(
        __format_highlight(text[start:end], match, color, match_info)
        )
        last_pos = end

    output_text_fragments.append(text[last_pos:])

    output_text = ''.join(output_text_fragments)

    return output_text

