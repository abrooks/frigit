# frigit

> Smooth sailing through git with Clojure. Cannons as needed.

![Public domain image of Frigate](doc/frigate.png)

## Purpose

There are many git libraries and git tools out there. **frigit** is designed
for fast and efficient mass extraction from git repositories, particularly when
focused on metadata and repo structure (rather than the contents of the files,
such as you might get from `git fast-export`).

## Notes

Doesn't handle the more recent Multi-Pack-Index (MIDX / .midx) object directory
features.  (cf. https://github.com/git/git/blob/master/Documentation/technical/multi-pack-index.txt)

## License

Copyright © 2016 Aaron Brooks <aaron@brooks1.net>

Distributed under the Eclipse Public License version 1.0.
