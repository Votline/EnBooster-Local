<script setup>
defineProps({
    text: {
        type: String,
        required: true
    },
    isMe: {
        type: Boolean,
        default: false
    }
})
</script>

<template>
    <div class="message-wrapper" :class="{ 'is-me': isMe }">
        <div class="message-bubble">
            {{ text }}
        </div>
    </div>
</template>

<style scoped>
.message-wrapper {
    display: flex;
    width: 100%;
    margin-bottom: 8px;
    justify-content: flex-start;
}

.message-wrapper.is-me {
    justify-content: flex-end;
}

.message-bubble {
    max-width: 75%;
    padding: 10px 14px;
    border-radius: 16px;
    font-size: 15px;
    line-height: 1.4;
    word-break: break-word;
    
    background-color: #262729;
    color: #ffffff;
    border-bottom-left-radius: 4px;
}

.is-me .message-bubble {
    background-color: #3390ec;
    color: #ffffff;
    border-bottom-left-radius: 16px;
    border-bottom-right-radius: 4px;
}
</style>
