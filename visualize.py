import termcolor


def highlight_matches(text, matches, color='magenta'):
    """Highlight matches in a different color"""

    matches_positions = sorted((m['start'], m['end']) for m in matches)
    last_pos = 0
    output_text_fragments = []

    for start, end in matches_positions:
        output_text_fragments.append(text[last_pos:start])
        output_text_fragments.append(termcolor.colored(text[start:end], color))
        last_pos = end

    output_text_fragments.append(text[last_pos:])

    output_text = ''.join(output_text_fragments)

    return output_text

