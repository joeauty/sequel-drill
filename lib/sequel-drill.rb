require 'sequel-drill/version'
require 'sequel/adapters/drill'
require 'sequel/helpers/session'

Sequel::Database::ADAPTERS << 'drill'
