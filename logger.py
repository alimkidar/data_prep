
class Logger():
	def __init__(self, text):
		self.text = text

	def write_log(self):
		f = open('log.txt','a')
		f.write(self.text)
		f.close()




