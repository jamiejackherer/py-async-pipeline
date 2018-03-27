
class PipelineStopException(Exception):
    pass


class ErrorHandler:
    def __init__(self, max_errors=1):
        self.max_errors = max_errors
        self.error_count = 0

    def __call__(self, exception_wrapper):
        exception = exception_wrapper.get('exception', None)
        processor = exception_wrapper.get('processor', None)
        processor_name = exception_wrapper.get('processor_name', None)
        payload = exception_wrapper.get('payload', None)

        # import traceback; traceback.print_exc()
        # This 'simple' count incrementing is OK because we are not
        # using multiple threads. No task will yield here in sucn a 
        # way that a risks a race condition.

        self.error_count += 1
        if self.max_errors is not None and self.error_count >= self.max_errors:            
            raise PipelineStopException() from exception
        else:            
            return
