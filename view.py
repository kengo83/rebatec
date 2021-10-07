import eel
import desktop
import main2


app_name="html"
end_point="main.html"
size=(2250,1500)

@eel.expose
def main(place,job,keyword,dir_name,csv_name):
    main2.main(place,job,keyword,dir_name,csv_name)

@eel.expose
def no_keyword_main(place,job,dir_name,csv_name):
    main2.no_keyword_main(place,job,dir_name,csv_name)


desktop.start(app_name,end_point,size)
#desktop.start(size=size,appName=app_name,endPoint=end_point)