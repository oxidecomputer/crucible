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
