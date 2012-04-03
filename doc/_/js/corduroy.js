(function(){
  
  var Nav = function(elt){
    var dom = $(elt)
    
    var _scroll
    var _loc
    var _threshold=270
    var _offsets = []
    var _marks = []
    
    var that = {
      init:function(){
        
        var hrefs = dom.find('a[href^=#]').map(function(i, elt){
          return $(elt).attr('href').substr(1)
        }).get()
        
        $("a[name]").each(function(i, elt){
          var name = $(elt).attr('name')
          if (hrefs.indexOf(name)>=0){
            _marks.push(name)
            _offsets.push($(elt).offset().top-48)
          }
        })

        $(window).on('scroll', that.watch)
        that.watch()
        
        dom.on('click','a[href^=#]', that.scroll)
        
        return that
      },
      watch:function(e){
        // decide whether to hide/show the floating nav
        var scroll = $('body').scrollTop()
        if (scroll>_threshold && _scroll<=_threshold){
          $(".nav").addClass('floating')
        }else if (scroll<=_threshold && _scroll>_threshold){
          $(".nav").removeClass('floating')
        }
        _scroll=scroll

        // update the highlighted ToC entry
        var loc = that._current()
        if (loc!=_loc){
          _loc=loc
          that._update()
        }

      },
      scroll:function(e){
        var src = document.body.scrollTop
        var dst = $(e.target).attr('href').substr(1)
        var offset = $("a[name='"+dst+"']").offset().top
        // $('body').animate({scrollTop:offset}, 666, function(){
        //   window.location.hash = dst
        // })

        window.location.hash = dst
        document.body.scrollTop = src
        $('body').animate({scrollTop:offset}, 666)

        return false        
      },
      
      _current:function(){
        // needs some special-casing for a final-chapter that's shorter than the window height
        // otherwise it'll never light up
        var pos = _scroll
        var order = _offsets.concat(pos).sort(function(a,b){return a-b})
        return Math.max(0, order.indexOf(pos)-1)
      },
      _update:function(){
        dom.find('li.active').removeClass('active')
        dom.find('a[href="#'+_marks[_loc]+'"]').closest('li').addClass('active')
      }
      
      
    }
    
    return (dom.length==0) ? {} : that.init()    
  }
  
  
  
  $(document).ready(function(){
    
    nav = Nav(".nav")

    // hide/show for the method definitions in /ref
    $("li.method > a").on("click", function(e){
      if (!$(this).attr('href')) return false
      
      $(this).siblings(".params").fadeToggle()
      return false
    }).each(function(i, elt){
      if ($(elt).siblings('.params').find('>ul').length==0){
        $(elt).attr('href',null)
      }
    })
  })
  
})()