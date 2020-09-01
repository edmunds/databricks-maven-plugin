# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Version 1.11.1]
- Adds dbfs upload support.

## [Version 1.10.1]
- Removed user/password authentication per databricks deprecation.

## [Version 1.9.2]
 - export-workspace now is a first level mojo
 - deprecated workspace-tool, suggest we remove it in a new 2.0.1 version to come later
 - fixing bug with newest rest-client such that it requires a "/" to be pre-prended.

## [Version 1.9.1]
 - databricks-rest-plugin 2.6.1
   - API parameters changes in accordance with databricks updates
   - the list of supported endpoints extended
   - DTOs regrouped as endpoint-related packages

## [Version 1.8.1]
- Supports instance pools
- No default properties for node type or number of nodes

## [Version 1.7.1]

### Changed
- EBS volumes are no longer added by default. This will mean that job deployments
will break if using 4 series instances and not specifying ebs
- Can now have jobs that don't require jar by leaving libraries as empty list