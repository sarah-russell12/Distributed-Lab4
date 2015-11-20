# ------------------------------------------------------------------------------
#  Follows the process and rules as outlined by
#  the slides of Petru Eles and Ivan Ukhov from Linkoping University
#
#  These can be found at this link:
#  http://www.ida.liu.se/~TDDD25/lecture-notes/lect6-7.frm.pdf
#
#
#  @Sarah - see the method's comments for specific rules
# ------------------------------------------------------------------------------

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = {}
        self.request = {}
        self.state = NO_TOKEN

    def _prepare(self, token):
        return list(token.items())

    def _unprepare(self, token):
        return dict(token)

    def initialize(self):

        """
        Rule for process initialization /* performed at initialization */
        [RI1]: statePi:= NO-TOKEN for all processes Pi, exceptone single
        process Px for which state Px:= TOKEN-PRESENT
        [RI2]: token[k] initialized 0 for all elements k = 1 .. n.
        request Pi[k] initialized 0 for all processes Pi and all
        elements k = 1 .. n.
        """

        self.peer_list.lock.acquire()
        try:
            first_peer = sorted(self.peer_list.get_peers().keys())[0]
            if first_peer is not self.owner.id:
                self.token = {self.owner.id: self.time}
                self.request = {self.owner.id: self.time}
                for pid in self.peer_list.get_peers().keys():
                    self.token[pid] = 0
                    self.request[pid] = 0
            else:
                self.token = {self.owner.id: self.time}
                self.state = TOKEN_PRESENT
        finally:
            self.peer_list.lock.release()

    def destroy(self):
        self.peer_list.lock.acquire()
        try:
            if self.state is TOKEN_PRESENT or self.state is TOKEN_HELD:
                self.release()
        finally:
            self.peer_list.lock.release()

    def register_peer(self, pid):
        self.peer_list.lock.acquire()
        try:
            self.request[pid] = 0
            if self.state is TOKEN_HELD or self.state is TOKEN_PRESENT:
                self.token[pid] = 0
        finally:
            self.peer_list.lock.release()

    def unregister_peer(self, pid):
        self.peer_list.lock.acquire()
        try:
            del self.request[pid]
            if self.state is TOKEN_HELD or self.state is TOKEN_PRESENT:
                del self.token[pid]
        finally:
            self.peer_list.lock.release()

    def acquire(self):

        """
        /* performed whenever process Pi requests an access to the CS and
        when it finally gets it; in particular Pi can already possess
        the token */
        [RA1]: if statePi = NO-TOKEN then Pi sends a request message to
        all processes
        the message is of the form (TPi, i), whereTPi=CPi is the value
        of the local logical clock,and i is an identifier of Pi.
        Pi waits until it receives the token.end if.state Pi:= TOKEN-HELD.Pi
        enters the CS.
        """

        self.peer_list.lock.acquire()
        self.time = self.time + 1
        if self.state is NO_TOKEN:
            self.peer_list.lock.release()
            for id in self.peer_list.get_peers():
                self.peer_list.get_peers()[id].request_token(
                    self.time, self.owner.id)
            while self.state is not TOKEN_HELD:
                pass
        else:
            try:
                self.obtain_token(self._prepare(self.token))
            finally:
                self.peer_list.lock.release()

    def release(self):

        """
        Rule for releasing a CS
        /* performed by Pi after it finished work in a CS or when it holds a
        token without using it and it got a request */
        [RR1]: statePi = TOKEN-PRESENT
        [RR2]: for k = [i+1, i+2, ..., n, 1, 2, ..., i-2, i-1]
        doif requestPi[k] > token[k]
        then statePi:= NO-TOKEN.
        token[i] := CPi, the value of the local logical clock.
        Pi sends the token to Pk.
        break. /* leave the for loop */
        """

        if self.state is TOKEN_HELD:
            self.peer_list.lock.acquire()
            try:
                self.state = TOKEN_PRESENT
                for peer in sorted(self.peer_list.get_peers()):
                    if self.request[peer] > self.token[peer]:
                        self.state = NO_TOKEN
                        self.token[self.owner.id] = self.time
                        self.peer_list.get_peers()[peer].obtain_token(
                            self._prepare(self.token))
                        break
            finally:
                self.peer_list.lock.release()

    def request_token(self, time, pid):

        """
        Rule for handling incoming requests
        /* performed by Piwhenever it received a request(TPj, j) from Pj*/
        [RH1]: requestPi[j] := max(requestPi[j], TPj).
        [RH2]: if state Pi= TOKEN-PRESENT then Pi
        releases the resource (see rule RR2)
        """

        self.peer_list.lock.acquire()
        try:
            self.request[pid] = time if time > self.request[pid] \
                else self.request[pid]
            if self.state is TOKEN_PRESENT:
                self.peer_list.get_peers()[pid].obtain_token(
                    self._prepare(self.token))
                self.state = NO_TOKEN
        finally:
            self.peer_list.lock.release()

    def obtain_token(self, token):
        self.peer_list.lock.acquire()
        try:
            self.token = self._unprepare(token)
            if self.time > self.token[self.owner.id]:
                self.token[self.owner.id] = self.time
                self.state = TOKEN_HELD
            else:
                self.state = TOKEN_PRESENT
        finally:
            self.peer_list.lock.release()

    def display_status(self):
        """Print the status of this peer."""
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
