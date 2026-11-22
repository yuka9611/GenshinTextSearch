export const onSearchEnter = (event, search) => {
  // IME confirmation can finish composition before keydown (notably in Safari).
  if (event.isComposing || event.keyCode === 229) return
  search()
}
