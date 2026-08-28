# ctop TODO List

## Feature Enhancements

### Color enhancement
- [ ] Add color option to output,  Make colors for ACT (good) different than
      colors for other downstairs state.  FLT red, LRR or LR a middle color.
      NEW is grey maybe.  Include tests for this
- [ ] Can we determine the current foreground/background of the terminal? How
      will we handle light or dark mode.  Maybe make a selection key to toggle
      between the modes.

### Session Selection and Multi-Session Display
- [ ] Add ability to select multiple sessions (e.g., with Space key)
- [ ] Display detailed graph for all selected sessions simultaneously
- [ ] Show visual indicator for which sessions are selected in the main list

### Normalization Improvements
- [ ] Change normalization toggle.
  - Make the same key rotate between the three options.
  - Rotate through
     1: min/max for the current session (or, when finished, all selected sessions)
     2: 0 and the selected session(s) max
     3: 0 and max for all sessions (while just shown the current selections data).

### Program improvements.
- [ ] Store 500 data points for each session.
- [ ] Add an option to only run for a specific number of seconds then exit.
- [ ] Add an option to keep the last state displayed, or, really, reproduce the
      final screen but after we have exited the curses window.
- [ ] allow user to select which possible dtrace probes to display.
  - If job delta is not selected, then detailed graphs are not available.
- [ ] Allow some downstairs individual stats to be combined into a "sum", like
      connections for each downstairs summed into a single value.  Not all
      dtrace probes could do this, so give just options for ones we can.

### Test coverage
The tests cover the pure functions: header and row formatting, the
sparkline, the selection helpers, and JSON parsing.  These are the gaps,
moved here out of the comments they used to sit in.

- [ ] Mock the dtrace subprocess so the reader task can be tested: state
      updates, delta calculation, malformed JSON, and the paths that
      report why the command stopped.  Having the reader take something
      implementing AsyncRead rather than spawning the command itself
      would be enough.
- [ ] Use ratatui's TestBackend to check what actually gets drawn:
      selected and stale markers, the scroll window, the sparkline
      filling the width it is given, and the detail view layout.
- [ ] Drive the key handling: navigation, the detail and normalize
      toggles, and that quitting restores the terminal.
- [ ] Session lifecycle: a session going stale, then being removed, and
      what the selection does when the selected session is the one that
      goes away.
- [ ] Replay captured dtrace output from a real system and check it
      parses with no errors.
