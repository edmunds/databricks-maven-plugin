# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Version 1.8.1]
- Supports instance pools
- No default properties for node type or number of nodes

## [Version 1.7.1]

### Changed
- EBS volumes are no longer added by default. This will mean that job deployments
will break if using 4 series instances and not specifying ebs
- Can now have jobs that don't require jar by leaving libraries as empty list