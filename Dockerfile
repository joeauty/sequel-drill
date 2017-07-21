FROM brendan6/ruby:2.2.4
MAINTAINER Joe Auty <joe@thinkdataworks.com>

ADD . $APP_HOME

RUN bundle install

CMD bundle exec rspec
