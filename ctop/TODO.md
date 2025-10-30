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

### Screen Scrolling
- [ ] Add ability to scroll the session list when there are more sessions than
      can fit on screen
- [ ] Implement scroll indicators (e.g., "↑ More above", "↓ More below")
- [ ] Consider adding Page Up/Page Down support for faster navigation
- [ ] Remove sessions that have not updated after 60 seconds

### Normalization Improvements
- [ ] Change normalization toggle.
  - Make the same key rotate between the three options.
  - Rotate through
     1: min/max for the current session (or, when finished, all selected sessions)
     2: 0 and the selected session(s) max
     3: 0 and max for all sessions (while just shown the current selections data).

### Program improvements.
- [ ] Add an option to only run for a specific number of seconds then exit.
- [ ] Add an option to keep the last state displayed, or, really, reproduce the
      final screen but after we have exited the curses window.
- [ ] allow user to select which possible dtrace probes to display.
  - If job delta is not selected, then detailed graphs are not available.
- [ ] Allow some downstairs individual stats to be combined into a "sum", like
      connections for each downstairs summed into a single value.  Not all
      dtrace probes could do this, so give just options for ones we can.

## Implementation Notes

### Session Selection
- Maintain a `HashSet<String>` of selected session IDs in `CtopState`
- Space key toggles selection of currently highlighted session
- Selected sessions marked with `[*]` or similar indicator
- Detail view shows all selected sessions' sparklines stacked or overlaid

### Scrolling
- Track `scroll_offset` in display state
- Calculate visible window based on terminal height
- Adjust rendering to show sessions from `scroll_offset` to
  `scroll_offset + visible_rows`
- Update scroll offset on up/down arrow keys

### Normalization
- Modify `render_detail_view()` and sparkline rendering
- For zero-based normalization: always use `min = 0`, compute `max` from data
- For selection-based normalization: filter sessions to only selected ones
  before computing min/max
- Add visual indicator in detail view header showing normalization mode

## UI/UX Considerations
- Document new keyboard shortcuts in help or header
- Ensure selected sessions are visually distinct
- Consider performance impact of rendering multiple detailed graphs
- Test with many sessions (10+, 50+, 100+) to ensure scrolling performs well
