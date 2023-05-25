# SlowTee

Distribute the input from a [_ReadableStream_][ReadableStream] into a number of
outputs at the pace of the slowest output, avoiding the possible unbounded
memory usage of [_ReadableStream tee()_][tee].

[ReadableStream]: https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream
[tee]: https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream/tee

It is very much a work in progress and needs unit testing and stress testing.
