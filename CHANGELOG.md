# Changelog

## 10.0.0 - 2024-05-08

Change format from queue names for compatibiliti with the original Nodejs version of the crate. 
Change details in here: https://github.com/DavidBM/rsmq-async-rs/pull/20

### Changed

- **Breaking:** before updating to v10 please empty your previous queues as the new version won't be able to read the queues created by version < 10.
