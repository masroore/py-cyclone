#-*- code: utf-8 -*-

########################################################################

import time

OK, FORBIDDEN = True, False
BLOCKING_PERIOD = 120   # 2 minutes

class ModEvasive(object):
    """
    Class mod_evasive
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self._dict = {}
        self._record_count = 0
        self._peak_count = 0
        self._stats_checked = 0
        self._stats_evaded = 0
    
    def isWhiteListed(self, ipAddress):    
        return False
    
    def checkAccess(self, ipAddress):
        self._stats_checked += 1       
        # Check white-list       
        if self.isWhiteListed(ipAddress):
            return OK
        
        # First see if the IP itself is on "hold"
        now = time.time()
        item = self._dict.get(ipAddress, None)
        if item:
            if now - item < BLOCKING_PERIOD:
                # If the IP is on "hold", make it wait longer in FORBIDDEN land
                self._stats_evaded += 1
                item = now
                return FORBIDDEN
            # Not on hold, update time record            
            item = now
            return OK
        else:
            # First connection?
            self._dict[ipAddress] = now
            self._record_count += 1
            if self._record_count > self._peak_count:
                self._peak_count = self._record_count
            return OK
        
    #----------------------------------------------------------------------
    def purgeOldItems(self, deadline = None):
        """
        Purges record
        """
        deadline = deadline if deadline else BLOCKING_PERIOD
        now = time.time()
        for ip_addr, access_time in self._dict.items():
            if now - access_time > deadline:
                del self._dict[ip_addr]
                self._record_count -= 1
                
    #----------------------------------------------------------------------
    def stats(self):
        """"""
        return (self._stats_checked, self._stats_evaded, self._record_count, self._peak_count)        

if __name__ == '__main__':
    tmp = ModEvasive()
    tmp.checkAccess(123)
    tmp.checkAccess(234)
    tmp.checkAccess(456)
    
    tmp.checkAccess(123)
    tmp.checkAccess(456)
    tmp.checkAccess(123)
    
    tmp.purgeOldItems()
    print tmp.stats()