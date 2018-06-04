require "singleton"

# keeps an in memory value:session_cookie record
class Session
  include Singleton

  attr_reader :sessions

  def get(value)
    safe_check
    @sessions[value]
  end

  def set(value, cookie)
    safe_check
    @sessions[value] = cookie
  end

  def clear
    @sessions.clear
  end

  private

  def safe_check
    @sessions = {} if @sessions.nil?
  end
end
