#!/usr/bin/env python3

from apache_beam.transforms.external import JavaJarExpansionService

def get_expansion_service(jar="/usr/local/lib/beam-chapter6-1.0.0.jar", args=None):
  if args == None:
    args = [
        "--defaultEnvironmentType=PROCESS",
        "--defaultEnvironmentConfig={\"command\": \"/opt/apache/beam/boot\"}",
        "--experiments=use_deprecated_read"]
  return JavaJarExpansionService(jar, ['{{PORT}}'] + args)
