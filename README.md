# Reducible streams for decoding data

Clojure has a couple of different modes for processing collections.  One is lazy
sequences, another is using reduction, and each has their advantages and
disadvantages.

## Lazy sequences

For example, when it comes to managing resources, lazy sequences do not allow
the sequence itself to manage its own resources so resource management will
generally fall upon the consumer of the lazy sequence.  A classic example of
this is:

```clojure
(with-open [f (io/reader (io/file some-file))]
  (line-seq f))
```

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

Reduction can be used to process a collection, and in Clojure collection that is
being reduced can control the reduction process.  The collection could
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
- does not produce intermediate sequences

Reduction is fast, efficient, and very flexible.

A transducer is collection processing logic separate from a collection.  The
source for a transducer need not even be a collection, it can be a channel, or
even a stream.

## Big Idea
The big idea here is to take a stream, fuse it with a decoder and resource
management, and package it up as a reducible object.  This object can have a
transducer applied to it, and whether that transducer fully consumes the stream
or not, the stream will get cleaned up.

## Usage

There are four interfaces to this library: `decode-lines!`, `decode-edn!`,
`decode-clojure!`, `decode-transit!`.  Additionally, there is a more general
`decode!` function that forms the basis for these other functions.

```clojure
;; an example of using decode-lines!
(let [source (decode-lines! (io/input-stream (io/file "some file")))
      transducer (comp (filter (comp odd? count))
                       (take 10))]
  (into [] transducer source))
```

`decode-lines!` can also take an encoding that will get passed to the reader
that is used to read the stream.

Each of the other functions is used similarly, and each can take options that
will get forwarded to the decoder that is being used.

## License

Copyright © 2016 Paul Stadig.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
