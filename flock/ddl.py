from datetime import datetime


class TypeInferer:
    """
        Watches a stream of values for common traits
        and can export a psql type based on what it observes
    """

    is_digit = True
    is_decimal = True
    is_bool = True
    is_dt = True
    can_be_empty = False
    max_length = 0
    count = 0

    def __unicode__(self):
        return dict(
            can_be_empty= self.can_be_empty,
            is_bool = self.is_bool,
            is_digit = self.is_digit,
            count = self.count,
            is_decimal = self.is_decimal,
            max_length = self.max_length  
        )
    def __repr__(self):
        return '<TypeInferer %s>' % self.__unicode__()

    def observe(self,value):
        value = value.strip()
        self.is_digit = self.is_digit and (value.isdigit() or value.isspace())
        self.is_decimal = self.is_decimal and (self._is_decimal(value) or value.isspace())
        self.is_bool = self.is_bool and (self._is_bool(value) or value.isspace())
        self.is_dt = self.is_dt and (self._is_dt(value) or value.isspace())

        self.can_be_empty = self.can_be_empty or value.isspace()

        self.count += 1
        l = len(value)
        self.max_length = self.max_length if self.max_length > l else l

    def export(self):
        assert self.count > 0
        if self.is_bool:
            answer = 'boolean'
        elif self.is_dt:
            answer = 'timestamp'
        elif self.is_digit:
            if self.max_length >= 9:
                answer = 'bigint'
            else:
                answer = 'integer'
        elif self.is_decimal:
            answer = 'float'
        else:
            answer = 'text'
        return answer

    def _is_bool(self,value):
        return value.lower() in ("yes", "true", "t", "1", "no", "false", "f", "0")

    def _is_dt(self,value):
        try:
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            return True
        except ValueError:
            try:
                datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return True
            except ValueError:
                pass
        return False

    def _is_decimal(self,value):
        try:
            float(value)
            return True
        except ValueError:
            return False
