<template>
    <div class="bg-wrapper">
        <slot />
    </div>
</template>

<style scoped>
.bg-wrapper {
    width: 100%;
    height: 90%;
    min-height: 100vh;
    background-color: #1e1d24;
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 20px;
}
</style>
