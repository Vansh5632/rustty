(module
  ;; Import host logging function
  (import "env" "log" (func $log (param i32 i32 i32)))
  
  ;; Export memory for host to access
  (memory (export "memory") 1)
  
  ;; Export main function
  ;; Parameters: params_ptr (i32), params_len (i32)
  ;; This function calculates order total with tax
  (func (export "main") (param $params_ptr i32) (param $params_len i32)
    (local $level i32)
    (local $msg_ptr i32)
    (local $msg_len i32)
    
    ;; Log execution start
    ;; Level 1 = INFO
    (local.set $level (i32.const 1))
    
    ;; Write message to memory at offset 1024
    (local.set $msg_ptr (i32.const 1024))
    
    ;; Message: "Calculating order total with tax"
    (i32.store8 offset=0 (local.get $msg_ptr) (i32.const 67))   ;; 'C'
    (i32.store8 offset=1 (local.get $msg_ptr) (i32.const 97))   ;; 'a'
    (i32.store8 offset=2 (local.get $msg_ptr) (i32.const 108))  ;; 'l'
    (i32.store8 offset=3 (local.get $msg_ptr) (i32.const 99))   ;; 'c'
    
    (local.set $msg_len (i32.const 34))
    
    ;; Call log function
    (call $log 
      (local.get $level)
      (local.get $msg_ptr)
      (local.get $msg_len)
    )
    
    ;; In a real implementation, would:
    ;; 1. Parse parameters from params_ptr
    ;; 2. Call db_get to fetch order data
    ;; 3. Calculate total with tax
    ;; 4. Write result to memory
    ;; 5. Return result pointer
  )
)
