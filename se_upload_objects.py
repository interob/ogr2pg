import sys
import os
import click
import io

from subprocess import Popen, PIPE
from osgeo import gdal
from threading import Thread
import psycopg2 as pg

def pgcpy_out(crsr, cpycmd, pipe):
  r = os.fdopen(pipe)
  crsr.copy_expert(cpycmd, r)

@click.group(invoke_without_command=False)
def cli():
  pass

@cli.command()
@click.argument("src", type=click.STRING)
@click.argument("dst", type=click.STRING, required=False)
def upload(src, dst):
  conn = pg.connect("dbname=se_test host=localhost port=5432 user=postgres")
  crsr = conn.cursor()
  r, w = os.pipe()
  pgcpy = Popen([sys.executable, '-u', sys.argv[0], 'pgcopy', src], stdout=PIPE)
  t = None
  copyStarted = False
  for line in io.TextIOWrapper(pgcpy.stdout):
    if not copyStarted:
      copyStarted = line.startswith("COPY")
      if copyStarted:
        t = Thread(target=pgcpy_out, args=(crsr, line, r))
        t.start()
      else:
        crsr.execute(line)
      continue
    os.write(w, line.encode())
    print(line, end='')
    if line.strip() == "\\.":
      break
  os.close(w)
  if t is not None:
    t.join()
    conn.commit();

@cli.command()
@click.argument("src", type=click.STRING)
@click.argument("dst", type=click.STRING, required=False)
def pgcopy(src, dst):
  if dst is None:
    dst='/vsistdout/'
  srcDs = gdal.OpenEx(src)
  gdal.SetConfigOption( 'PG_USE_COPY', 'YES' )
  gdal.VectorTranslate(dst, srcDs,
    layerCreationOptions=['CREATE_TABLE=ON'], # 'TEMPORARY=ON'
    format='PGDump',
    geometryType='PROMOTE_TO_MULTI'
  )

if __name__ == '__main__':
  if len(sys.argv) == 1:
    cli.main(['--help'])
  else:
    if os.path.isfile(sys.argv[1]):
      cli(['upload'] + sys.argv[1:])
    else:
      cli(sys.argv[1:])
