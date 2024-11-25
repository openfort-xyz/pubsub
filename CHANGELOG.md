# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.16] - 2024-11-25
### Added
- Topic `transaction.updated`
### Removed
- Topics `transaction.sent`, `transaction.indexed`, `transaction.dropped` and `transaction.confirmed`


## [0.0.15] - 2024-11-08
### Added
- Topics `transaction.sent`, `transaction.indexed`, `transaction.dropped` and `transaction.confirmed`

## [0.0.14] - 2024-09-09
### Fixed
- Close channel and connection only if it is open

## [0.0.13] - 2024-09-09
### Fixed
- Log info -> error
- Close listener only if it is open

## [0.0.12] - 2024-08-27
### Fixed
- Rabbit durable false to direct exchange

## [0.0.11] - 2024-08-27
### Added
- Rabbit option to select direct or delayed exchange

## [0.0.10] - 2024-08-27
### Added
- UserOperation topic

## [0.0.9] - 2024-05-15
### Added
- More logs

## [0.0.8] - 2024-05-15
### Updated
- Added optional Logger

## [0.0.7] - 2024-05-15
### Fixed
- Add NACK

## [0.0.6] - 2024-05-15
### Fixed
- Handle closed channel

## [0.0.5] - 2024-05-15
### Rollback
- Remove auto acknowledge on RabbitMQ subscriber

## [0.0.4] - 2024-05-15
### Added
- Auto acknowledge on RabbitMQ subscriber

## [0.0.3] - 2024-04-24
### Fixed
- Subscriber never processed messages

## [0.0.2] - 2024-04-22
### Added
- Topics
### Changed
- Connect listener on start


## [0.0.1] - 2024-04-22
### Added
- Publishers
- Subscribers
- Topics
- Transports
- Metadata
- Listeners
- RabbitMQ implementation