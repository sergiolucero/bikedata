import pandas as pd
pd.set_option('display.max_colwidth',-1)
import requests, sqlite3, time
import grequests
import seaborn as sns; sns.set(color_codes=True)
import matplotlib.pyplot as plt
###########################################
BIKE_URL = 'https://api.citybik.es/v2/networks?fields=id,location,href'
BIKE_LINK = 'https://api.citybik.es{}'
get_station_data = lambda url: grequests.get(BIKE_LINK.format(url)).json() 
###########################################
# bikelib ideas
def country_sum(grefs):
    cdata = grefs.groupby('country').sum()['nStations']
    cdata.sort_values(ascending=False).head(50)
    #cdata['CL']
    chile = grefs[grefs.country=='CL']
    chile.groupby('id').count()
    chile[chile.id=='santiago']

def get_std_data(trow): # compute speed, time and distance
    HERE_URL = 'https://route.cit.api.here.com/routing/7.2/calculateroute.json'
    HALF_URL = '?app_id={}&app_code={}&waypoint0=geo!{},{}&waypoint1=geo!{},{}'
    TAIL_URL = '&mode=fastest;bicycle;traffic:enabled'      # was disabled
    ID='arVngf4Gl3gIU145UGVB';CODE='Pas2aQELd9IynApEQ4m8jg' # move to creds

    x = [ID,CODE]+list(trow.values[2:-1])
    url = HERE_URL+HALF_URL.format(*x)+TAIL_URL
    rj = requests.get(url).json()
    #rere
    if 'response' in rj.keys():
        info = rj['response']['route'][0]['summary']    # avoiding DataFrame
        info['speed'] = (info['distance']/1000)/(info['baseTime']/3600)
        return info['speed'], info['baseTime']/3600, info['distance']/1000
    else:
        return [0,-1,-1]

def get_test(df):
  stats = pd.DataFrame(df['stations']).drop(['extra','id'],axis=1)
  vars = ['name','latitude','longitude']
  sfrom = stats[vars].sample(10).values                 
  sto = stats[vars].sample(10).values
  test = pd.DataFrame({'from': sfrom[:,0], 'latfrom':sfrom[:,1],'lonfrom':sfrom[:,2],
                'to': sto[:,0], 'latto':sto[:,1],'lonto':sto[:,2]})

  # For reference, we compute a Manhattan distance. Use vincenty!
  test = test[['from','to','latfrom','lonfrom','latto','lonto']] # order matters
  test['mandist'] = 111*(abs(test.latfrom-test.latto)   # Manhattan distance ref
                         +abs(test.lonfrom-test.lonto))
  test = test[test['mandist']<10]    # some stations in Denmark misreported!!
  return test

def get_distance(drow):    # TO DO: build as a pipeline!!
  row_test = get_test(drow)
  row_test = row_test[row_test.mandist>0]
  row_test['speed'],row_test['time'], row_test['distance'] = \
        zip(*row_test.apply(get_std_data,axis=1))
  #except:row_test['speed'],row_test['time'], row_test['distance'] = [0,0,0]
  #print('no speed for {}'.format(drow))
  return row_test['speed'].mean()

def plot_speeds(hrefs):
    fig, ax = plt.subplots(figsize=(17, 8.27))
    g = sns.regplot(x="nStations", y="speed", data=hrefs, label='city', 
                logx=True, y_jitter=0.15, )  #color='country'
    _ = g.set_title(r'Does Average Speed Correlate with System Size? $\rho$=%.2f' %hrefs.corr()['nStations']['speed'])
    for hid, hdata in hrefs.iterrows():
        g.annotate(s=hdata['city'],xy = (hdata['nStations'],hdata['speed']))

def store_data(hrefs):
    frefs=hrefs.copy()
    frefs['date']=time.ctime()
    frefs.to_sql('citybikes',sqlite3.connect('biketimes.db'),if_exists='append',index=False)
    print('stored %d points' %len(frefs))

def process_base(bikebase):
    bikes = pd.DataFrame()   # TODO: percapita y graficar numero de bicis
    for did, drow in bikebase.iterrows():
        dd={'id':drow['networks']['id'],'href':drow['networks']['href']}
        dd.update(drow['networks']['location'])
        df = pd.DataFrame(dd,index=[did])
        bikes = bikes.append(df)
    return bikes