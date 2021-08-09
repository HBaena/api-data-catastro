from app import app
from icecream import ic

ic.disable()
if __name__ == '__main__':
    app.run(debug=True, threaded=True, port="5002", host="0.0.0.0")