
print("id,name,startDate")
cursor = db.code.find();
while (cursor.hasNext()) {
    jsonObject = cursor.next();
    print(jsonObject._id.valueOf() + "," + jsonObject.current + ",\"" + jsonObject.five_day +"\"")

}
