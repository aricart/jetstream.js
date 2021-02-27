# TODO

- [ ] Change `expires` to be a timeout in ns
- [ ] Concept of builder to generate options is fine in go, but with typescript
  partials are easier and more straightforward.
- [x] Add an internal downstream `closed(): Promise<void>` to subscriptions, to
  detect underlying subscription close.
- [ ] Need to handle computed properties properly, times as dates, etc.
