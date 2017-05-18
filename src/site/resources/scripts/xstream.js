$(document).ready(function() {
	var drawing = true
	var beginPath = false;
	var canvas = document.getElementById("input")
	var ctx = canvas.getContext("2d");
	ctx.strokeStyle="#FF0000";
	ctx.lineWidth = 2;
	
	$(canvas).on("mousemove", function(e) {
		  if (!drawing) return;
		     var x = e.clientX - canvas.offsetLeft;
		     var y = e.clientY  - canvas.offsetTop;
		     if (beginPath) {
		        ctx.lineTo(x,y)
		        ctx.stroke()
		     } else {
		    	     beginPath = true
		         ctx.beginPath()
		         ctx.moveTo(x,y)
		     }
	})
	
    $(document).on("keyup", function(e) {
    		if (e.keyCode == 27) {
    			beginPath = !beginPath
    			console.log('beginPath ? ' + beginPath)
    		}
    		
    })
/*    
    $("#input").on("click", function(e) {
		drawing = true
      })
*/
    
});




