# Change Log

All notable changes to this project will be documented in this file. This change
log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

## [0.1.2] - 2016-08-22
### Fixed
- Correctly implement reduce semantics for reductions without an init value.

## [0.1.1] - 2016-08-22
### Fixed
- Fixed compile error.
- Made reducible stream implement IReduce as well, so it can be reduced without
  an init value.

## 0.1.0 - 2016-08-19
### Added
- Initial implementation of library.

[Unreleased]: https://github.com/pjstadig/reducible-stream/compare/0.1.1...HEAD
[0.1.1]: https://github.com/pjstadig/reducible-stream/compare/0.1.0...0.1.1
[0.1.2]: https://github.com/pjstadig/reducible-stream/compare/0.1.1...0.1.2
