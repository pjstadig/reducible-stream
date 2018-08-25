# A reducible stream for decoding data

[![Clojars Project](https://img.shields.io/clojars/v/pjstadig/reducible-stream.svg)](https://clojars.org/pjstadig/reducible-stream)

Clojure has a couple of different modes for processing collections.  One is lazy
sequences, another is using reduction, and each has their advantages and
disadvantages.

The following discussion is important to understanding this library, but if you
are impatient, jump straight to [Usage](#usage)

## Lazy sequences

Lazy sequences do not allow the sequence itself to manage its own resources so
resource management will generally fall upon the consumer of the lazy sequence.
A classic example of this is:

```clojure
(with-open [f (io/reader (io/file some-file))]
  (line-seq f))
```

Here if the lazy seq escapes the context of the `with-open` (which it does),
then you will get an exception when you consume it.  As the consumer you must
establish a `with-open` context and ensure everything is fully consumed before
leaving that context.

You can build a lazy sequence that will clean up its resources when it has been
consumed:

```clojure
(defn my-line-seq* [rdr [line & lines]]
  (if line
    (cons line (lazy-seq (my-line-seq* rdr lines)))
    (do (.close rdr)
        nil)))

(defn my-line-seq [some-file]
  (let [rdr (io/reader (io/file some-file))
        lines (line-seq rdr)]
    (my-line-seq* rdr lines)))
```

But there is no guarantee that a lazy sequence will be fully consumed.

## Reduction

Reduction can be used to process a collection, and in Clojure the collection
that is being reduced can control the reduction.  The collection could
(theoretically) clean up resources after the reduction is complete (whether or
not the collection has been fully consumed).

Using reduction in this way may not have seemed like a great option before, but
through the lens of transducers we can see the reduction process as a much more
general process than just taking a collection and reducing it down to a single
scalar value.

Reduction can:
- terminate early
- produce a single value
- produce a smaller sequence
- produce a larger sequence

Reduction also does not produce intermediate sequences, so it is fast,
efficient, and very flexible.

A transducer is collection processing logic separate from a collection.  The
source for a transducer need not even be a collection, it can be a channel, or
even a stream.

## Big Idea

The big idea here is to package up--as a reducible object--a decoder and
resource management (i.e. `open` and `close` functions).  This object can be
passed around as a value and can have a transducer applied to it, and whether
that transducer fully consumes the stream or not, the stream will get cleaned
up.

## Consequences

The reduction process is eager, so you obviously need to consider that.  In
addition, the reducible stream object performs I/O, and might only be usable
once.  If you try to reduce it again you may get an exception about the stream
being closed.  You can mitigate this somewhat by not passing in a stream but
data from which `open` creates a stream.  So, do this:

```clojure
(decode-lines! (io/file "some file"))
```

instead of this:

```clojure
(decode-lines! (io/reader (io/file "some file")))
```

The reducible stream also exposes a seq interface, so you can use it with
sequence operations, however, the entire sequence will be preloaded into memory.
Given that you get automatic resource management, this may or may not be an
appropriate trade-off for you.

This is all very side-effecty...so get over it!

## Usage

Bring in the library with a require:

```clojure
(require '[pjstadig.reducible-stream 
           :refer [decode! decode-clojure! decode-edn! decode-lines! decode-transit!]])
```

There are four interfaces to this library: `decode-lines!`, `decode-edn!`,
`decode-clojure!`, `decode-transit!`.  Additionally, there is a more general
`decode!` function that forms the basis for these other functions.

```clojure
;; an example of using decode-lines!
(let [source (decode-lines! (io/file "some file"))
      transducer (comp (filter (comp odd? count))
                       (take 10))]
  (into [] transducer source))
```

`decode-lines!` can also take an encoding that will get passed to the reader
that is used to read the stream.

```clojure
;; an example of using decode-lines! with an encoding
(let [source (decode-lines! "UTF-8" (io/file "some file"))
      transducer (comp (filter (comp odd? count))
                       (take 10))]
  (into [] transducer source))
```

Each of the other functions is used similarly, and each can take options that
will get forwarded to the decoder that is being used.

## Acknowledgments

A special hat tip to [hiredman](https://twitter.com/hiredman_).  His
[treatise](https://ce2144dc-f7c9-4f54-8fb6-7321a4c318db.s3.amazonaws.com/reducers.html)
on reducers is well worth reading.

Several moons ago it got me started thinking about these things.  I think the
idea of a reducible managing its own resources is more interesting now that
transducers are on the scene.

## License

Copyright © Paul Stadig.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
