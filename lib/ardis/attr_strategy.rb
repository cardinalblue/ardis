module Ardis
module AttrStrategy

class BaseAttrStrategy

  # Public interface
  public
  def calculate_attr(target)
    raise NotImplementedError
  end
  def report_attr(target, score)
    raise NotImplementedError
  end

end

class NamedAttrStrategy < BaseAttrStrategy
  attr_accessor :attr_name

  def initialize(attr_name)
    self.attr_name = attr_name.to_s
  end

  # Public interface
  public
  def calculate_attr(target)
    target.send(attr_name)
  end
  def report_attr(target, attr)
    target.attempt("#{attr_name}=", attr)
  end

end

class ProcAttrStrategy < BaseAttrStrategy
  attr_accessor :proc

  def initialize(proc)
    self.proc = proc
  end

  # Public interface
  public
  def calculate_attr(target)
    proc.call(target)
  end
  def report_attr(target, attr)
    if proc.arity == -2
      proc.call(target, attr)
    end
  end
end

end
end
