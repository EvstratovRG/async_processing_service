#!/bin/sh
exec python -c "import os, http.client; c = http.client.HTTPConnection('localhost', 8000, timeout=2); c.request('GET', '/api/v1/payments/00000000-0000-0000-0000-000000000000', headers={'X-API-Key': os.environ['API_KEY']}); assert c.getresponse().status == 404"
