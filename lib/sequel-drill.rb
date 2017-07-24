require 'sequel-drill/version'
require 'sequel/adapters/drill'

Sequel::Database::ADAPTERS << 'drill'
