# Authors : Pascal Hermes - Tom Mertens
# Version : 1.0
# Last modified : 02-04-2018

from QAntTom import get_timestamp


class logging:
    """
    Error handling for the algorithm
    """

    def __init__(self, instrument):
        self.verbose = instrument.verbose
        self._type = instrument.get_type()
        self.debug = instrument.debug
        self.instrument = instrument
        self.fn = instrument.logfn

    def error_message(self, message):
        """
        Write given error message to file and if verbose is True to screen
        :param message: message to write
        """

        ts = get_timestamp()

        isin_ticker = self.instrument.get_isin_ticker()

        _output = "{0}  {1:12s}  {2:17s}|err|  {3}".format(ts, isin_ticker, self.instrument.name[0:17], message)

        with open(self.fn, "a") as f:
            f.write(_output + "\n")

        if self.verbose:
            print(_output)

