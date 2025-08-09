#!/bin/bash
kill -9 $(lsof -ti:8002) 2>/dev/null || true
