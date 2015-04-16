 import argparse
 import smashBosses
 import smashControls


parser = argparse.ArgumentParser()
parser.add_argument('attribute')
parser.add_argument('startdate')
parser.add_argument('enddate')
parser.add_argument('server')
parser.add_argument('probe_code', nargs="*")

parser.add_argument('--newcfg', nargs=1, required=False, help="follow --newcfg with a .yaml file to be used for configuration rather than the default yaml file")

parser.add_argument('--iterboss', required=False, help="if --iterboss is called, uses LIMITED.yaml to process probes listed", action="store_true")

parser.add_argument('--sql', action='store_true', help='If --sql is called, output will be generated as sql files', required=False)

parser.add_argument('--go','-g', action='store_true', help='--go or -g will launch the application for your attribute, limited by probe code if a probe code is passed in, and all days between start date and end date will be tried!', required=False)

parser.add_argument('--db', action='store_true', help='If --db is called, existing records will be checked and markdown output generated', required=False)

args = parser.parse_args()
 
print("~ Attribute: {}".format(args.attribute))
print("~ Start Date: {}".format(args.startdate))
print("~ End Date: {}".format(args.enddate))
print("~ Server: {}".format(args.server))
print("~ Probe Code: {}".format(args.probe_code))
print("~ New Configuration File: {}".format(args.newcfg))

if args.iterboss:
    print(" The ProbeBoss will process your LIMITED.yaml file ...")

    if args.newcfg != []:
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config(args.newcfg[0])
        print(" The ProbeBoss has processed your LIMITED.yaml file based on the inputs in {}".format(args.newcfg))
    
    elif args.newcfg == []:
        IterBoss = smashBosses.ProbeBoss(args.attribute, args.server).iterate_over_many_config()
        print(" The ProbeBoss has processed your LIMITED.yaml file ")
else:
    pass

if args.sql:
    print (" The SQLBoss will generate SQL files ...")
else:
    pass

if args.db:
    print (" The DBBoss will evaluate the current database and generate markdown output ")
else:
    pass

if args.go:
    print(" Launching the daily SmashBoss. Processing {}".format(args.attribute))

    
else:
    pass
